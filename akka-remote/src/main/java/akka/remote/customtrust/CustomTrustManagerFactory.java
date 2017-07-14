package akka.remote.customtrust;

import javax.net.ssl.TrustManager;

public interface CustomTrustManagerFactory {
    TrustManager[] create(String keystorePath, String keystorePassword);
}
