package org.example.elasticsearchtest.repository.document;

import org.example.elasticsearchtest.document.UserDocument;
import org.springframework.data.elasticsearch.annotations.Query;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

public interface UserDocRepository extends ElasticsearchRepository<UserDocument, String> {
    @Query("{\"match\": {\"name\": \"?0\"}}")
    List<UserDocument> findByNameMatch(String name);
}
