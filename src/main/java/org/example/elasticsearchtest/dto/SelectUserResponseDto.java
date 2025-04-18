package org.example.elasticsearchtest.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SelectUserResponseDto {
    private String id;
    private String name;
    private String userId;
}
