package org.example.elasticsearchtest.repository.jpa;

import org.example.elasticsearchtest.entity.UserEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface UserJPARepository extends JpaRepository<UserEntity, Long> {
}
