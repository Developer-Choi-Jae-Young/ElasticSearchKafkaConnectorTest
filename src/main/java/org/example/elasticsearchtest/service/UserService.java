package org.example.elasticsearchtest.service;

import lombok.RequiredArgsConstructor;
import org.example.elasticsearchtest.document.UserDocument;
import org.example.elasticsearchtest.dto.*;
import org.example.elasticsearchtest.entity.UserEntity;
import org.example.elasticsearchtest.repository.document.UserDocRepository;
import org.example.elasticsearchtest.repository.jpa.UserJPARepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

@Service
@RequiredArgsConstructor
public class UserService {
    private final UserJPARepository userJPARepository;
    private final UserDocRepository userDocRepository;

    public InsertUserResponseDto insertUser(InsertUserRequestDto insertUserRequestDto) {
        UserEntity entity = UserEntity.builder()
                                        .userId(insertUserRequestDto.getUserId())
                                        .name(insertUserRequestDto.getName())
                                        .password(insertUserRequestDto.getPassword())
                                        .updatedAt(new Date())
                                        .build();

        UserEntity resultEntity = userJPARepository.save(entity);

        return InsertUserResponseDto.builder().id(resultEntity.getId()).userId(resultEntity.getUserId()).password(resultEntity.getPassword()).name(resultEntity.getName()).build();
    }

    @Transactional
    public UpdateUserResponseDto updateUser(UpdateUserRequestDto userRequestDto) {
        UserEntity entity = userJPARepository.findById(userRequestDto.getOriginalUserNo()).orElseThrow(() -> new RuntimeException("updateUser Function is call but is Null!"));

        entity.setUserId(userRequestDto.getChangeUserId());
        entity.setName(userRequestDto.getChangeName());
        entity.setPassword(userRequestDto.getChangePassword());
        entity.setUpdatedAt(new Date());

        return UpdateUserResponseDto.builder().id(entity.getId()).userId(entity.getUserId()).name(entity.getName()).password(entity.getPassword()).build();
    }

    @Transactional
    public void deleteUser(Long userNo) {
        userJPARepository.deleteById(userNo);
    }

    public List<SelectUserResponseDto> selectUserAll() {
        Iterable<UserDocument> documents = userDocRepository.findAll();

        List<SelectUserResponseDto> selectUserResponseDtos = new ArrayList<>();
        for (UserDocument doc : documents) {
            selectUserResponseDtos.add(SelectUserResponseDto.builder().id(doc.getId()).userId(doc.getUserId()).name(doc.getName()).build());
        }

        return selectUserResponseDtos;
    }

    public List<SelectUserResponseDto> selectUserNameMatch(String name) {
        return userDocRepository.findByNameMatch(name).stream().map(doc -> SelectUserResponseDto.builder().id(doc.getId()).userId(doc.getUserId()).name(doc.getName()).build()).toList();
    }
}
