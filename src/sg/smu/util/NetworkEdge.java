package sg.smu.util;

import org.jgrapht.graph.DefaultEdge;

public class NetworkEdge extends DefaultEdge {
    private String v1;
    private String v2;

    public NetworkEdge(String v1, String v2) {
        this.v1 = v1;
        this.v2 = v2;
        
    }

    public String getV1() {
        return v1;
    }

    public String getV2() {
        return v2;
    }

  
}