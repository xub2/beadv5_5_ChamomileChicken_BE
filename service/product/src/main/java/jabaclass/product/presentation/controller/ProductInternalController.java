package jabaclass.product.presentation.controller;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import jabaclass.product.application.usecase.ProductUseCase;
import jabaclass.product.application.usecase.ScheduleUseCase;
import jabaclass.product.common.exception.ApiResponseDto;
import jabaclass.product.presentation.dto.request.ProductBulkReadRequestDto;
import jabaclass.product.presentation.dto.request.SearchProductRequestDto;
import jabaclass.product.presentation.dto.response.ProductSettlementItemResponseDto;
import jabaclass.product.presentation.dto.response.ScheduleStartDateResponseDto;
import jabaclass.product.presentation.dto.response.SearchProductResponseDto;
import jabaclass.product.presentation.openapi.ProductInternalOpenApi;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/products")
public class ProductInternalController implements ProductInternalOpenApi {

	private final ProductUseCase productUseCase;
	private final ScheduleUseCase scheduleUseCase;

	@Override
	@PostMapping("/bulk")
	public ResponseEntity<List<ProductSettlementItemResponseDto>> getProductsByIds(
		@Valid @RequestBody ProductBulkReadRequestDto requestDto
	) {
		return ResponseEntity.ok(productUseCase.getProductsByIds(requestDto.productIds()));
	}

	@PostMapping("/es-migrate")
	public ResponseEntity<Map<String, Integer>> migrateToEs() {
		int count = productUseCase.migrateToEs();
		return ResponseEntity.ok(Map.of("indexed", count));
	}

	@GetMapping("/search-db")
	public ResponseEntity<ApiResponseDto<SearchProductResponseDto>> searchByDb(
		@ModelAttribute SearchProductRequestDto request) {
		SearchProductResponseDto response = productUseCase.searchByDb(request);
		return ResponseEntity.ok()
			.body(ApiResponseDto.success(HttpStatus.OK, "성공적으로 검색되었습니다.", response));
	}

	@GetMapping("/schedules/{scheduleId}/start-date")
	public ResponseEntity<ScheduleStartDateResponseDto> getScheduleStartDate(@PathVariable UUID scheduleId) {
		return ResponseEntity.ok(new ScheduleStartDateResponseDto(scheduleUseCase.getScheduleStartDate(scheduleId)));
	}
}
