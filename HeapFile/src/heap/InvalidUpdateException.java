package heap;
import chainexception.ChainException;

public class InvalidUpdateException extends Throwable {

        public InvalidUpdateException(Exception ex, String name)
        { 
          super(name); 
        }
       

}
