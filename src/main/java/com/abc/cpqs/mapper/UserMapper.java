package com.abc.cpqs.mapper;

import com.abc.cpqs.domain.User;

import java.util.List;

/**
 * Created by lijiax on 6/18/15.
 */
public interface UserMapper {
    public void insertUser(User user);

    public User getUserById(Integer userId);

    public List<User> getAllUsers();

    public void updateUser(User user);

    public void deleteUser(Integer userId);
}
