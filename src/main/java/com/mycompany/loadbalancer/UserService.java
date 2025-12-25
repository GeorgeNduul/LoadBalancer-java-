
package com.mycompany.loadbalancer;


import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class UserService {
    public enum Perm { READ, WRITE }
    public static class ShareEntry {
        public final String owner, target, filename; public final Perm perm;
        public ShareEntry(String owner, String target, String filename, Perm perm) { this.owner=owner; this.target=target; this.filename=filename; this.perm=perm; }
    }

    private final Map<String, User> users = new ConcurrentHashMap<>();
    private final List<ShareEntry> shares = Collections.synchronizedList(new ArrayList<>());

    public UserService() { users.put("admin", new User("admin", "admin", User.Role.ADMIN)); }

    public boolean createUser(String name, String pass, User.Role role) {
        if (users.containsKey(name)) return false; users.put(name, new User(name, pass, role)); return true;
    }
    public boolean updateUser(String name, String pass, User.Role role) {
        User u = users.get(name); if (u == null) return false; if (pass != null) u.password = pass; if (role != null) u.role = role; return true;
    }
    public boolean deleteUser(String name) { return users.remove(name) != null; }
    public boolean promoteToAdmin(String name) { User u = users.get(name); if (u == null) return false; u.role = User.Role.ADMIN; return true; }

    public User auth(String userHeader, String passHeader) {
        if (userHeader == null) return null;
        User u = users.get(userHeader);
        return (u != null && (passHeader == null || passHeader.equals(u.password))) ? u : null;
    }

    public boolean isOwner(String requester, String filename) { return filename.startsWith(requester + ":"); }
    public boolean canRead(String requester, String filename) {
        if (isOwner(requester, filename)) return true;
        synchronized (shares) {
            return shares.stream().anyMatch(s -> s.target.equals(requester) && s.filename.equals(filename) && (s.perm == Perm.READ || s.perm == Perm.WRITE));
        }
    }
    public boolean canWrite(String requester, String filename) {
        if (isOwner(requester, filename)) return true;
        synchronized (shares) {
            return shares.stream().anyMatch(s -> s.target.equals(requester) && s.filename.equals(filename) && s.perm == Perm.WRITE);
        }
    }
    public boolean share(String owner, String to, String filename, Perm perm) {
        if (!isOwner(owner, filename)) return false; shares.add(new ShareEntry(owner, to, filename, perm)); return true;
    }
}
