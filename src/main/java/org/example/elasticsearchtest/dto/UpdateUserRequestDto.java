package org.example.elasticsearchtest.dto;

import lombok.Data;

@Data
public class UpdateUserRequestDto {
    private Long originalUserNo;
    private String changeUserId;
    private String changePassword;
    private String changeName;
}
