package org.example.elasticsearchtest.controller;

import lombok.RequiredArgsConstructor;
import org.example.elasticsearchtest.dto.*;
import org.example.elasticsearchtest.service.UserService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequiredArgsConstructor
public class MainController {
    private final UserService userService;

    @PostMapping("/user/insert")
    public InsertUserResponseDto insertUser(@RequestBody InsertUserRequestDto insertUserRequestDto) {
        return userService.insertUser(insertUserRequestDto);
    }

    @PostMapping("/user/update")
    public UpdateUserResponseDto updateUser(@RequestBody UpdateUserRequestDto updateUserRequestDto) {
        return userService.updateUser(updateUserRequestDto);
    }

    @PostMapping("/user/delete/{no}")
    public String deleteUser(@PathVariable Long no) {
        userService.deleteUser(no);
        return "deleteUser function call and process done.";
    }

    @GetMapping("/user/select/all")
    public List<SelectUserResponseDto> selectUserAll() {
        return userService.selectUserAll();
    }

    @GetMapping("/user/select/match/{name}")
    public List<SelectUserResponseDto> selectUserNameMatch(@PathVariable String name) {
        return userService.selectUserNameMatch(name);
    }
}
