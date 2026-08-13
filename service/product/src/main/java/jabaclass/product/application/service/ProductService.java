package jabaclass.product.application.service;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import jabaclass.product.application.acl.SellerRepository;
import jabaclass.product.application.exception.BusinessException;
import jabaclass.product.application.usecase.ProductUseCase;
import jabaclass.product.application.usecase.ValidateFileUseCase;
import jabaclass.product.common.exception.CommonErrorCode;
import jabaclass.product.domain.model.Product;
import jabaclass.product.domain.model.ProductImageItem;
import jabaclass.product.domain.model.status.ProductStatus;
import jabaclass.product.domain.repository.ProductRepository;
import jabaclass.product.domain.repository.ProductSearchRepository;
import jabaclass.product.infrastructure.acl.dto.response.UserResponseDto;
import jabaclass.product.infrastructure.elasticsearch.ProductDocument;
import jabaclass.product.infrastructure.event.dto.ProductAiSyncedEvent;   // 추가
import jabaclass.product.infrastructure.event.dto.ProductDeletedEvent;    // 추가
import jabaclass.product.infrastructure.event.dto.ProductEventResponseDto;
import jabaclass.product.infrastructure.event.dto.ProductViewedEvent;
import jabaclass.product.infrastructure.kafka.ProductEsIndexMessage;
import jabaclass.product.infrastructure.outbox.EsEventType;
import jabaclass.product.infrastructure.outbox.OutboxEvent;
import jabaclass.product.infrastructure.outbox.OutboxRepository;
import jabaclass.product.presentation.dto.request.CreateProductRequestDto;
import jabaclass.product.presentation.dto.request.SearchProductRequestDto;
import jabaclass.product.presentation.dto.request.UpdateProductRequestDto;
import jabaclass.product.presentation.dto.response.DeleteProductResponseDto;
import jabaclass.product.application.dto.FileConfirmResponse;
import jabaclass.product.presentation.dto.response.ProductResponseDto;
import jabaclass.product.presentation.dto.response.ProductSettlementItemResponseDto;
import jabaclass.product.presentation.dto.response.SearchProductResponseDto;

@Service
@Transactional(readOnly = true)
@RequiredArgsConstructor
@Slf4j
public class ProductService implements ProductUseCase {
	private final ProductRepository productRepository;
	private final ProductSearchRepository productSearchRepository;
	private final SellerRepository sellerRepository;
	private final ApplicationEventPublisher publisher;
	private final ValidateFileUseCase validateFileUseCase;
	private final OutboxRepository outboxRepository;
	private final ObjectMapper objectMapper;

	@Override
	@Transactional
	public ProductResponseDto create(CreateProductRequestDto requestDto, UUID sellerId) {
		List<ProductImageItem> images = List.of();
		if (requestDto.imageIds() != null && !requestDto.imageIds().isEmpty()) {
			List<FileConfirmResponse> confirmed = requestDto.imageIds().stream()
				.map(validateFileUseCase::validateAndConfirm)
				.toList();
			images = confirmed.stream()
				.map(r -> new ProductImageItem(r.fileId(), r.storagePath()))
				.toList();
		}

		Product product = Product.builder()
			.sellerId(requestDto.sellerId())
			.title(requestDto.title())
			.maxCapacity(requestDto.maxCapacity())
			.description(requestDto.description())
			.price(requestDto.price())
			.status(requestDto.status())
			.roadAddress(requestDto.roadAddress())
			.detailAddress(requestDto.detailAddress())
			.zonecode(requestDto.zonecode())
			.latitude(requestDto.latitude())
			.longitude(requestDto.longitude())
			.build();

		product.changeImages(images);

		Product saved = productRepository.save(product);
		UserResponseDto seller = findBySellerIdOrThrow(sellerId);

		publisher.publishEvent(new ProductEventResponseDto(saved.getId()));
		publisher.publishEvent(ProductAiSyncedEvent.from(saved));           // 추가
		saveEsSaveOutbox(saved, seller.name());
		return ProductResponseDto.from(saved, seller.name());
	}
	@Override
	@Transactional
	public ProductResponseDto update(UpdateProductRequestDto requestDto, UUID productId, UUID sellerId) {
		Product product = findByIdOrThrow(productId);

		matchProductAndSellerId(productId, sellerId);

		product.changeTitle(requestDto.title());
		product.changeMaxCapacity(requestDto.maxCapacity());
		product.changeDescription(requestDto.description());
		product.changePrice(requestDto.price());
		product.changeStatus(requestDto.status());
		product.changeRoadAddress(requestDto.roadAddress());
		product.changeDetailAddress(requestDto.detailAddress());
		product.changeZonecode(requestDto.zonecode());
		product.changeLatitude(requestDto.latitude());
		product.changeLongitude(requestDto.longitude());

		// 이미지 수정 — null이면 기존 이미지 유지
		if (requestDto.imageIds() != null) {
			List<ProductImageItem> images = List.of();
			if (!requestDto.imageIds().isEmpty()) {
				List<FileConfirmResponse> confirmed = requestDto.imageIds().stream()
					.map(validateFileUseCase::validateAndConfirm)
					.toList();
				images = confirmed.stream()
					.map(r -> new ProductImageItem(r.fileId(), r.storagePath()))
					.toList();
			}
			product.changeImages(images);
		}
		UserResponseDto seller = findBySellerIdOrThrow(sellerId);
		publisher.publishEvent(ProductAiSyncedEvent.from(product));         // 추가
		saveEsSaveOutbox(product, seller.name());
		return ProductResponseDto.from(product, seller.name());
	}

	@Override
	@Transactional
	public DeleteProductResponseDto delete(UUID productId, UUID sellerId) {
		// 상품 존재하는지 확인
		Product product = findByIdOrThrow(productId);
		// 본인 상품인지 확인
		matchProductAndSellerId(productId, sellerId);

		product.changeStatus(ProductStatus.DISABLE);
		product.changeDelete();
		publisher.publishEvent(ProductDeletedEvent.of(productId));          // 추가
		saveEsDeleteOutbox(productId.toString());

		return DeleteProductResponseDto.from(productId, ProductStatus.DISABLE);
	}

	// es 추가
	@Override
	@CircuitBreaker(name = "elasticsearchCB", fallbackMethod = "searchAllFallback")
	public SearchProductResponseDto searchAll(SearchProductRequestDto requestDto) {
		Pageable pageable = PageRequest.of(requestDto.thisPage(), requestDto.pageSize());

		Page<ProductDocument> page;
		if (requestDto.title() == null || requestDto.title().isBlank()) {
			page = productSearchRepository.findAllEnabled(pageable);
		} else {
			page = productSearchRepository.searchByKeyword(requestDto.title(), pageable);
		}

		// ES 문서에 sellerName이 비정규화되어 있어 user 서비스 추가 호출 불필요
		List<ProductResponseDto> content = page.getContent().stream()
			.map(ProductResponseDto::from)
			.toList();

		return SearchProductResponseDto.fromEs(page, content);
	}

	@Override
	public SearchProductResponseDto searchByDb(SearchProductRequestDto requestDto) {
		return doDbSearch(requestDto);
	}

	private SearchProductResponseDto searchAllFallback(SearchProductRequestDto requestDto, Throwable t) {
		log.warn("ES 장애 감지 — DB fallback 실행. circuit: elasticsearchCB", t);
		return doDbSearch(requestDto);
	}

	private SearchProductResponseDto doDbSearch(SearchProductRequestDto requestDto) {
		Pageable pageable = PageRequest.of(requestDto.thisPage(), requestDto.pageSize());

		Page<Product> page;
		if (requestDto.title() == null || requestDto.title().isBlank()) {
			page = productRepository.findByStatusAndDeleteDtIsNull(ProductStatus.ENABLE, pageable);
		} else {
			page = productRepository.findByStatusAndTitleContainingAndDeleteDtIsNull(ProductStatus.ENABLE,
				requestDto.title(), pageable);
		}

		if (page.isEmpty()) {
			return SearchProductResponseDto.from(page, List.of());
		}

		List<UUID> sellerIds = page.getContent().stream().map(Product::getSellerId).distinct().toList();
		Map<UUID, String> sellerNameMap = sellerRepository.findSellerList(sellerIds)
			.map(list -> list.stream()
				.collect(Collectors.toMap(UserResponseDto::userId, UserResponseDto::name, (existing, replacement) -> existing)))
			.orElse(Map.of());

		List<ProductResponseDto> content = page.getContent().stream()
			.map(p -> ProductResponseDto.from(p, sellerNameMap.getOrDefault(p.getSellerId(), "")))
			.toList();

		return SearchProductResponseDto.from(page, content);
	}

	@Override
	@CircuitBreaker(name = "elasticsearchCB", fallbackMethod = "searchMyFallback")
	public SearchProductResponseDto searchMy(SearchProductRequestDto requestDto, UUID sellerId) {
		Pageable pageable = PageRequest.of(requestDto.thisPage(), requestDto.pageSize());
		String sellerIdStr = sellerId.toString();

		Page<ProductDocument> page;
		if (requestDto.title() == null || requestDto.title().isBlank()) {
			page = productSearchRepository.findAllBySellerId(sellerIdStr, pageable);
		} else {
			page = productSearchRepository.searchByKeywordAndSellerId(requestDto.title(), sellerIdStr, pageable);
		}

		List<ProductResponseDto> content = page.getContent().stream()
			.map(ProductResponseDto::from)
			.toList();

		return SearchProductResponseDto.fromEs(page, content);
	}

	private SearchProductResponseDto searchMyFallback(SearchProductRequestDto requestDto, UUID sellerId, Throwable t) {
		log.warn("ES 장애 감지 — DB fallback 실행. circuit: elasticsearchCB", t);
		Pageable pageable = PageRequest.of(requestDto.thisPage(), requestDto.pageSize());

		Page<Product> page;
		if (requestDto.title() == null || requestDto.title().isBlank()) {
			page = productRepository.findBySellerIdAndStatusAndDeleteDtIsNull(sellerId, ProductStatus.ENABLE, pageable);
		} else {
			page = productRepository.findBySellerIdAndStatusAndTitleContainingAndDeleteDtIsNull(sellerId,
				ProductStatus.ENABLE, requestDto.title(), pageable);
		}

		if (page.isEmpty()) {
			return SearchProductResponseDto.from(page, List.of());
		}

		UserResponseDto seller = findBySellerIdOrThrow(sellerId);
		List<ProductResponseDto> content = page.getContent().stream()
			.map(p -> ProductResponseDto.from(p, seller.name()))
			.toList();

		return SearchProductResponseDto.from(page, content);
	}

	@Override
	public ProductResponseDto searchById(UUID productId, UUID userId) {
		Product product = findByIdOrThrow(productId);

		UserResponseDto seller = findBySellerIdOrThrow(product.getSellerId());
		if (userId != null) {
			log.info("상품 조회 이벤트 발행 준비: userId={}, productId={}", userId, productId);
			publisher.publishEvent(ProductViewedEvent.of(userId, productId));
			log.info("상품 조회 이벤트 발행 완료: userId={}, productId={}", userId, productId);
		}

		return ProductResponseDto.from(product, seller.name());
	}

	@Override
	public Product findByIdOrThrow(UUID productId) {
		return productRepository.findById(productId)
			.orElseThrow(() -> new BusinessException(CommonErrorCode.PRODUCT_NOT_FOUND));
	}

	@Override
	// 해당 상품 보유자인지 확인
	public Product matchProductAndSellerId(UUID productId, UUID sellerId) {
		return productRepository.findByIdAndSellerId(productId, sellerId)
			.orElseThrow(() -> new BusinessException(CommonErrorCode.MATCH_FAIL));
	}

	@Override
	public List<ProductSettlementItemResponseDto> getProductsByIds(List<UUID> productIds) {
		if (productIds == null || productIds.isEmpty()) {
			return List.of();
		}

		List<UUID> distinctProductIds = productIds.stream()
			.distinct()
			.toList();

		Map<UUID, Product> productMap = productRepository.findAllByIds(distinctProductIds).stream()
			.collect(Collectors.toMap(Product::getId, product -> product));

		return distinctProductIds.stream()
			.map(productMap::get)
			.filter(Objects::nonNull)
			.map(ProductSettlementItemResponseDto::from)
			.toList();
	}

	@Override
	public int migrateToEs() {
		final int BATCH_SIZE = 100;
		int pageNum = 0;
		int totalIndexed = 0;

		while (true) {
			Pageable pageable = PageRequest.of(pageNum, BATCH_SIZE);
			Page<Product> page = productRepository.findAllByDeleteDtIsNull(pageable);

			if (page.isEmpty()) {
				break;
			}

			List<UUID> sellerIds = page.getContent().stream()
				.map(Product::getSellerId)
				.distinct()
				.toList();

			Map<UUID, String> sellerNameMap = sellerRepository.findSellerList(sellerIds)
				.map(list -> list.stream()
					.collect(Collectors.toMap(UserResponseDto::userId, UserResponseDto::name)))
				.orElse(Map.of());

			List<ProductDocument> documents = page.getContent().stream()
				.map(p -> ProductDocument.from(p, sellerNameMap.getOrDefault(p.getSellerId(), "")))
				.toList();

			productSearchRepository.saveAll(documents);
			totalIndexed += documents.size();
			log.debug("ES 마이그레이션 진행 중: {}페이지, 누적 {}건", pageNum, totalIndexed);

			if (page.isLast()) {
				break;
			}
			pageNum++;
		}

		log.info("ES 마이그레이션 완료: 총 {}건", totalIndexed);
		return totalIndexed;
	}

	private void saveEsOutbox(String aggregateId, EsEventType eventType, Object message) {
		try {
			String payload = objectMapper.writeValueAsString(message);
			outboxRepository.save(OutboxEvent.create("PRODUCT", aggregateId, eventType, payload));
		} catch (JsonProcessingException e) {
			log.error("ES outbox 직렬화 실패: type={}, id={}", eventType, aggregateId, e);
			throw new RuntimeException("ES outbox 직렬화 실패", e);
		}
	}

	private void saveEsSaveOutbox(Product product, String sellerName) {
		saveEsOutbox(product.getId().toString(), EsEventType.ES_SAVE,
			ProductEsIndexMessage.save(ProductDocument.from(product, sellerName)));
	}

	private void saveEsDeleteOutbox(String productId) {
		saveEsOutbox(productId, EsEventType.ES_DELETE, ProductEsIndexMessage.delete(productId));
	}

	// 로그인 계정 여부
	private UserResponseDto findBySellerIdOrThrow(UUID sellerId) {
		UserResponseDto sellerInfo = sellerRepository.findSeller(sellerId)
			.orElseThrow(() -> new BusinessException(CommonErrorCode.SELLER_NOT_FOUND));

		return sellerInfo;
	}

}
