package org.example.elasticsearchtest.dto;

import lombok.Data;

@Data
public class InsertUserRequestDto {
    private String userId;
    private String password;
    private String name;
}
