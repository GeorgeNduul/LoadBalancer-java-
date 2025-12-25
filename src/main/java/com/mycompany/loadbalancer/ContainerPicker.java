
package com.mycompany.loadbalancer;
import java.util.List;
public interface ContainerPicker {
    FileContainer choose(List<FileContainer> healthy);
    String name();
}
