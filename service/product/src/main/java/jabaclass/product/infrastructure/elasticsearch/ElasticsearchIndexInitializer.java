package jabaclass.product.infrastructure.elasticsearch;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.IndexOperations;
import org.springframework.stereotype.Component;

import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class ElasticsearchIndexInitializer {

	private final ElasticsearchOperations elasticsearchOperations;

	@Value("${elasticsearch.index.recreate-on-startup:false}")
	private boolean recreateOnStartup;

	@PostConstruct
	public void init() {
		IndexOperations indexOps = elasticsearchOperations.indexOps(ProductDocument.class);

		if (recreateOnStartup) {
			if (indexOps.exists()) {
				indexOps.delete();
				log.info("products 인덱스 삭제 (recreate-on-startup 설정)");
			}
			indexOps.createWithMapping();
			log.info("products 인덱스 재생성 완료");
			return;
		}

		if (!indexOps.exists()) {
			indexOps.createWithMapping();
			log.info("products 인덱스 생성 완료");
			return;
		}

		// 매핑 변경 감지
		Map<String, Object> currentMapping = indexOps.getMapping(); // 지금 ES에 실제로 저장되어 있는 매핑
		Map<String, Object> expectedMapping = indexOps.createMapping(); // 현재 ProductDocument 기준으로 "원래 이래야 하는" 매핑
		if (!currentMapping.equals(expectedMapping)) {
			log.warn("products 인덱스 매핑 불일치 감지. elasticsearch.index.recreate-on-startup=true 로 재생성하거나 수동으로 재색인하세요.");
		}
	}
}
