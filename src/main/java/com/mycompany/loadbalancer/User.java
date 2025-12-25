
package com.mycompany.loadbalancer;
public class User {
    public enum Role { STANDARD, ADMIN }
    public final String name;
    public String password;
    public Role role;
    public User(String name, String pass, Role role) { this.name = name; this.password = pass; this.role = role; }
}