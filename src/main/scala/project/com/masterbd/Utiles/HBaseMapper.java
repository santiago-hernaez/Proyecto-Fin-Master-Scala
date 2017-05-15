package project.com.masterbd.Utiles;


import org.apache.flink.api.common.functions.Function;

import java.io.Serializable;

/**
 * Maps a input value to a row in HBase table.
 *
 * @param <IN> input type
 */
public interface HBaseMapper<IN> extends Function, Serializable {

    /**
     * Given an input value return the HBase row key. Row key cannot be null.
     *
     * @param value
     * @return row key
     */
    byte[] getRowKey(IN value);

    /**
     * Given an empty {@link MutationActions} to which new actions can be added based on an input value.
     *
     * @param value
     * @param mutActions an empty {@link MutationActions}
     */
    void addActions(IN value, MutationActions mutActions);
}