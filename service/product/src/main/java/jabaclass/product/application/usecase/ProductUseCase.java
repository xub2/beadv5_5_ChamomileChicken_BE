package jabaclass.product.application.usecase;

import java.util.List;
import java.util.UUID;

import jabaclass.product.domain.model.Product;
import jabaclass.product.presentation.dto.request.CreateProductRequestDto;
import jabaclass.product.presentation.dto.request.SearchProductRequestDto;
import jabaclass.product.presentation.dto.request.UpdateProductRequestDto;
import jabaclass.product.presentation.dto.response.DeleteProductResponseDto;
import jabaclass.product.presentation.dto.response.ProductResponseDto;
import jabaclass.product.presentation.dto.response.ProductSettlementItemResponseDto;
import jabaclass.product.presentation.dto.response.SearchProductResponseDto;

public interface ProductUseCase {

	// 상품 생성
	ProductResponseDto create(CreateProductRequestDto requestDto, UUID sellerId);

	// 상품 수정
	ProductResponseDto update(UpdateProductRequestDto requestDto, UUID productId, UUID sellerId);

	// 상품 삭제
	DeleteProductResponseDto delete(UUID productId, UUID sellerId);

	// 상품 전체 검색
	SearchProductResponseDto searchAll(SearchProductRequestDto requestDto);

	// 판매자 본인 상품 검색
	SearchProductResponseDto searchMy(SearchProductRequestDto requestDto, UUID sellerId);

	// 특정 상품 검색
	ProductResponseDto searchById(UUID productId, UUID userId);

	// 상품 존재 여부/단일 상품 검색
	Product findByIdOrThrow(UUID productId);

	// 해당 상품 보유자인지 확인
	Product matchProductAndSellerId(UUID productId, UUID sellerId);

	List<ProductSettlementItemResponseDto> getProductsByIds(List<UUID> productIds);

	// PostgreSQL → ES 초기 마이그레이션
	// 새 상품은 생성/수정 시점에 자동으로 ES에 색인되지만, ES 도입 이전에 이미 RDB에 쌓인 상품들은 ES에 없는 상태라 검색이 안 됨. 이걸 한 번에 밀어넣기 위한 엔드포인트
	int migrateToEs();

	// DB 직접 검색 (부하 테스트용 — Full Scan / GIN 인덱스 성능 측정 엔드포인트)
	SearchProductResponseDto searchByDb(SearchProductRequestDto requestDto);
}
