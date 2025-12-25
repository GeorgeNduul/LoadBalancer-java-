/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.loadbalancer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author ntu-user
 */
public class Catalog {
    
// Fields (use your actual class names)
private final Map<String, Set<FileContainer>> fileToContainers = new HashMap<>();
private final Map<FileContainer, Set<String>> containerToFiles = new HashMap<>();

public synchronized Set<String> filesOn(FileContainer c) {
    return new HashSet<>(containerToFiles.getOrDefault(c, java.util.Collections.emptySet()));
}

public synchronized void addReplica(String fileName, FileContainer c) {
    fileToContainers.computeIfAbsent(fileName, k -> new HashSet<>()).add(c);
    containerToFiles.computeIfAbsent(c, k -> new HashSet<>()).add(fileName);
}

public synchronized void removeReplica(String fileName, FileContainer c) {
    Set<FileContainer> set = fileToContainers.get(fileName);
    if (set != null) {
        set.remove(c);
        if (set.isEmpty()) fileToContainers.remove(fileName);
    }

    Set<String> files = containerToFiles.get(c);
    if (files != null) {
        files.remove(fileName);
        if (files.isEmpty()) containerToFiles.remove(c);
    }
}

    
}
