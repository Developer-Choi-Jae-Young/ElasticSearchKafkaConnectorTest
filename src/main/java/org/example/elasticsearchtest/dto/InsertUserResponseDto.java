package org.example.elasticsearchtest.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class InsertUserResponseDto {
    private Long id;
    private String userId;
    private String password;
    private String name;
}
