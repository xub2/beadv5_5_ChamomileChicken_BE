package jabaclass.product.infrastructure.elasticsearch;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import jabaclass.product.domain.model.status.ProductStatus;
import jabaclass.product.domain.repository.ProductSearchRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.BulkFailureException;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.query.Criteria;
import org.springframework.data.elasticsearch.core.query.CriteriaQuery;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.core.query.IndexQueryBuilder;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.UpdateQuery;
import org.springframework.data.elasticsearch.core.query.ByQueryResponse;

@Repository
@RequiredArgsConstructor
@Slf4j
public class ProductSearchRepositoryAdapter implements ProductSearchRepository {

	private final ElasticsearchOperations elasticsearchOperations;

	@Override
	public ProductDocument save(ProductDocument document) {
		return elasticsearchOperations.save(document);
	}

	@Override
	public void saveAll(List<ProductDocument> documents) {
		List<IndexQuery> queries = documents.stream()
			.map(doc -> new IndexQueryBuilder()
				.withId(doc.getId())
				.withObject(doc)
				.build())
			.toList();
		try {
			elasticsearchOperations.bulkIndex(queries, ProductDocument.class);
		} catch (BulkFailureException e) {
			log.error("ES bulk 색인 일부 실패. 실패 문서: {}", e.getFailedDocuments());
		}
	}

	@Override
	public void deleteById(String id) {
		elasticsearchOperations.delete(id, ProductDocument.class);
	}

	@Override
	public Page<ProductDocument> searchByKeyword(String keyword, Pageable pageable) {
		// CriteriaQuery or().and() 체이닝 시 우선순위 문제로 NativeQuery bool 쿼리 사용
		// (title OR description) AND status=ENABLE AND deleted=false
		Query query = Query.of(q -> q
			.bool(b -> b // 두가지 이상 조건 은 bool 쿼리 써야함
				.should(s -> s.match(m -> m.field("title").query(keyword).fuzziness("AUTO").prefixLength(1)))
				.should(s -> s.match(m -> m.field("description").query(keyword)))
				.minimumShouldMatch("1")
				.filter(f -> f.term(t -> t.field("status").value(ProductStatus.ENABLE.name())))
				.filter(f -> f.term(t -> t.field("deleted").value(false)))
			)
		);

		NativeQuery nativeQuery = NativeQuery.builder()
			.withQuery(query)
			.withPageable(pageable)
			.build();

		SearchHits<ProductDocument> hits = elasticsearchOperations.search(nativeQuery, ProductDocument.class);
		List<ProductDocument> content = hits.stream().map(SearchHit::getContent).toList();
		return new PageImpl<>(content, pageable, hits.getTotalHits());
	}

	@Override
	public Page<ProductDocument> findAllEnabled(Pageable pageable) {
		Criteria criteria = new Criteria("status").is(ProductStatus.ENABLE.name())
			.and(new Criteria("deleted").is(false));

		CriteriaQuery query = new CriteriaQuery(criteria, pageable);
		SearchHits<ProductDocument> hits = elasticsearchOperations.search(query, ProductDocument.class);
		List<ProductDocument> content = hits.stream().map(SearchHit::getContent).toList();
		return new PageImpl<>(content, pageable, hits.getTotalHits());
	}

	@Override
	public Page<ProductDocument> findAllBySellerId(String sellerId, Pageable pageable) {
		Criteria criteria = new Criteria("status").is(ProductStatus.ENABLE.name())
			.and(new Criteria("deleted").is(false))
			.and(new Criteria("sellerId").is(sellerId));

		CriteriaQuery query = new CriteriaQuery(criteria, pageable);
		SearchHits<ProductDocument> hits = elasticsearchOperations.search(query, ProductDocument.class);
		List<ProductDocument> content = hits.stream().map(SearchHit::getContent).toList();
		return new PageImpl<>(content, pageable, hits.getTotalHits());
	}

	@Override
	public void updateSellerNameForAll(String sellerId, String newName) {
		CriteriaQuery criteriaQuery = new CriteriaQuery(new Criteria("sellerId").is(sellerId));

		UpdateQuery updateQuery = UpdateQuery.builder(criteriaQuery)
			.withScript("ctx._source.sellerName = params.newName")
			.withParams(Map.of("newName", newName))
			.build();

		ByQueryResponse response = elasticsearchOperations.updateByQuery(
			updateQuery, IndexCoordinates.of("products"));
		log.info("ES sellerName 업데이트 완료: sellerId={}, 업데이트 건수={}", sellerId, response.getUpdated());
	}

	@Override
	public Page<ProductDocument> searchByKeywordAndSellerId(String keyword, String sellerId, Pageable pageable) {
		Query query = Query.of(q -> q
			.bool(b -> b
				.should(s -> s.match(m -> m.field("title").query(keyword).fuzziness("AUTO").prefixLength(1)))
				.should(s -> s.match(m -> m.field("description").query(keyword)))
				.minimumShouldMatch("1")
				.filter(f -> f.term(t -> t.field("status").value(ProductStatus.ENABLE.name())))
				.filter(f -> f.term(t -> t.field("deleted").value(false)))
				.filter(f -> f.term(t -> t.field("sellerId").value(sellerId)))
			)
		);

		NativeQuery nativeQuery = NativeQuery.builder()
			.withQuery(query)
			.withPageable(pageable)
			.build();

		SearchHits<ProductDocument> hits = elasticsearchOperations.search(nativeQuery, ProductDocument.class);
		List<ProductDocument> content = hits.stream().map(SearchHit::getContent).toList();
		return new PageImpl<>(content, pageable, hits.getTotalHits());
	}
}
