package mapping;

import java.rmi.Remote;

public interface Memento extends Remote {
  Object getFieldValue(String fieldName) throws Exception;
}
