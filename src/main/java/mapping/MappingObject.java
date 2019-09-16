package mapping;

import java.rmi.RemoteException;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Properties;

public interface MappingObject extends/* Remote,*/ Cloneable {
  Boolean isEditable();
  void setEditable(Boolean editable);
  
  String getComments();
  void setComments(String comments);
  
  Long getLoadId();
  
  boolean isUpdate(Map jsonMemento) throws RemoteException;

  enum Type{PROJECT,CURRENT,ARCHIVE}
  enum RemoveAction{DELETE,MOVE_TO_ARCHIVE,MARK_FOR_DELETE}
  
  Class getInterface() throws RemoteException;

  Properties getParams() throws RemoteException;
  void setParams(Properties params) throws RemoteException;
  
  Integer getId() throws RemoteException;
  void setId(Integer id) throws RemoteException;
  
  String getName() throws RemoteException;
  void setName(String name) throws RemoteException;
  
  Type getType() throws RemoteException;
  void setType(Type type) throws RemoteException;
  
  Timestamp getDate() throws RemoteException;
  void setDate(Timestamp date) throws RemoteException;
  
  Timestamp getModificationDate() throws RemoteException;
  void setModificationDate(Timestamp modificationDate) throws RemoteException;

  String getLastUserName() throws RemoteException;
  void setLastUserName(String lastUserName) throws RemoteException;
  
  Integer getLastUserId() throws RemoteException;
  void setLastUserId(Integer lastUserId) throws RemoteException;
  
  Boolean isTmp() throws RemoteException;
  void setTmp(Boolean tmp) throws RemoteException;
 
  void setFixTime(Timestamp fixTime) throws RemoteException;
  Timestamp getFixTime() throws RemoteException;
  boolean isFixTime() throws RemoteException;
  String getRealClassName() throws RemoteException;
}
