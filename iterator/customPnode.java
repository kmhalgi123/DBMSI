package iterator;


public class customPnode {
    private pnode node;
    private int customTS;
    public customPnode(pnode node, int customTS){
        this.node = node;
        this.customTS = customTS;
    }

    public pnode getPnode(){
        return this.node;
    }

    public int getTimestamp(){
        return this.customTS;
    }
}