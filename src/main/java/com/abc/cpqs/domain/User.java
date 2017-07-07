package com.abc.cpqs.domain;

import java.io.Serializable;

/**
 * Created by lijiax on 6/18/15.
 */
public class User implements Serializable{
    private static final long serialVersionUID = 1779371791340041L;
    private int id;
    private String name;
    private String email;
    private String country;
    private String password;


    public void setID(int id){
        this.id = id;
    }

    public int getId(){
        return this.id;
    }
    public void setName(String name){
        this.name = name;
    }
    public String getName(){
        return this.name;
    }
    public void setEmail(String email){
        this.email = email;
    }
    public String getEmail(){
        return this.email;
    }
    public void setCountry(String country){
        this.country = country;
    }
    public String getCountry(){
        return this.country;
    }
    public void setPassword(String password){
        this.password = password;
    }
    public String getPassword(){
        return this.password;
    }

    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", email='" + email + '\'' +
                ", country='" + country + '\'' +
                ", password='" + password + '\'' +
                '}';
    }

    public void printUser(){
        System.out.println(id+name+email+country+password);
    }
}
