package akka.remote.customtrust;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.security.GeneralSecurityException;

import javax.net.ssl.TrustManager;

public class TrustManagerReflectionHelper
{
    public static CustomTrustManagerFactory getFactoryInstance(String className) throws GeneralSecurityException
    {
        try
        {
            Class<?> tmpClass = Class.forName(className);
            Class<? extends CustomTrustManagerFactory> factoryClass = tmpClass.asSubclass(CustomTrustManagerFactory.class);
            Constructor<? extends CustomTrustManagerFactory> constructor = factoryClass.getConstructor();
            return constructor.newInstance();
        }
        catch (ClassNotFoundException|NoSuchMethodException|IllegalAccessException|InstantiationException|InvocationTargetException e)
        {
            throw new GeneralSecurityException("Failed to instantiate custom trust manager factory", e);
        }
    }

    public static TrustManager[] create(String className, String path, String password) throws GeneralSecurityException
    {
        return getFactoryInstance(className).create(path, password);
    }
}
