package bum.pool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DivisionObjectPool {
  private final ExecutorService threadPool = Executors.newSingleThreadExecutor();
  public ArrayList<SoftReference> objects = new ArrayList<>();
  private final Class objectClass;
  private Logger log = LoggerFactory.getLogger(DivisionObjectPool.class);

  public DivisionObjectPool(Class objectClass) {
    this.objectClass = objectClass;
  }
  
  public Object getObject() {
    if(objects.isEmpty()) {
      threadPool.submit(new ObjectTask());
      return createObject();
    }else return objects.remove(0).get();
  }
  
  private Object createObject() {
    try {
      return objectClass.newInstance();
    }catch(InstantiationException | IllegalAccessException ex) {
      log.error("", ex);
    }
    return null;
  }
  
  private class ObjectTask implements Runnable {
    @Override
    public void run() {
      for(int i=0;i<20;i++)
        objects.add(new SoftReference(createObject()));
    }
  }
}
