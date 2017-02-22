package filter.statedistrict;

import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

public class ObjectivesBPL extends FilterFunc {
    @Override
    public Boolean exec(Tuple input) throws IOException {
        try {
            int getobjbpl = (int) input.get(0);
            int getperfbpl = (int) input.get(1);

            if (getobjbpl > getperfbpl){   

                if (((getobjbpl - getperfbpl)/getobjbpl)*100 >= 80) {
                    return true;
                } else {
                    return false;   
                }
            } else {
                return true;
            }
        } catch (ExecException ee) {
            throw ee;
        }
    }
} 
