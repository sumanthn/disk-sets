package sn.analytics.sets.type;

import sn.analytics.sets.TypeHint;

import java.io.Serializable;

/**
 * Stores info on type of elements
 * expected elements & other details
 * Created by sumanth
 */
public class SetMeta implements Serializable {

    //name is unique across sets
    private String name;
    private int expectedElements;
    private TypeHint typeHint;

    public SetMeta() {

    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getExpectedElements() {
        return expectedElements;
    }

    public void setExpectedElements(int expectedElements) {
        this.expectedElements = expectedElements;
    }

    public TypeHint getTypeHint() {
        return typeHint;
    }

    public void setTypeHint(TypeHint typeHint) {
        this.typeHint = typeHint;
    }

    @Override
    public String toString() {
        return "SetMeta{" +
                "name='" + name + '\'' +
                ", expectedElements=" + expectedElements +
                ", typeHint=" + typeHint +
                '}';
    }

   /* public static void main(String [] args){
        SetMeta sm = new SetMeta();
        sm.setName("pset1");
        sm.setExpectedElements(10000);
        sm.setTypeHint(TypeHint.STRING);
        Gson gson = new Gson();
        System.out.println(gson.toJson(sm));
    }*/
}
