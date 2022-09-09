import com.crystal.model.User;
import com.crystal.proxy.IServices;
import com.crystal.proxy.Services;
import com.crystal.proxy.ServicesInvocationHandler;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;

public class ProxyAdminTest {


        User user = new User("Indrit", 21, "admin");

        ServicesInvocationHandler invocationHandler = new ServicesInvocationHandler(new Services(), user);

        IServices services = (IServices) Proxy.newProxyInstance(IServices.class.getClassLoader(), new Class[]{IServices.class}, invocationHandler);

        @Test
        public void testGuestsMethods() {
            Assertions.assertDoesNotThrow(() -> services.multiply(1));
        }
        @Test
        public void testUserMethods()
        {
            Assertions.assertDoesNotThrow( () -> services.addOneUser(5));
        }
        @Test
        public void testAdminMethods(){
            Assertions.assertDoesNotThrow(() -> services.sqrtAdmin(5));
        }

}
