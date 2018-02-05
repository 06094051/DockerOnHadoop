package org.apache.hadoop.fs.http.server;

import com.mtyun.Constants;
import com.mtyun.auth.KeystoneManager;
import com.mtyun.auth.TokenCredential;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.server.AuthenticationHandler;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.hadoop.fs.http.client.HttpFSFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.BASE64Decoder;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by zhuochen on 17/8/11.
 */
public class KeystoneAuthenticationHandler implements AuthenticationHandler {

    private static Logger LOG = LoggerFactory.getLogger(HttpFSAuthenticationFilter.class);

    /**
     * Constant that identifies the authentication mechanism.
     */
    public static final String TYPE = Constants.AUTH_TYPE_KEYSTONE;

    private KeystoneManager keystoneManager;
    private Cipher cipher;
    private boolean tenantNameAsUserHome;

    public KeystoneAuthenticationHandler() {
    }

    @Override
    public void init(Properties config) throws ServletException {
        String authUrl = config.getProperty(Constants.CONF_KEYSTONE_URL);
        String userName = config.getProperty(Constants.CONF_KEYSTONE_USERNAME);
        String passWord = config.getProperty(Constants.CONF_KEYSTONE_PASSWORD);

        String key = config.getProperty(Constants.CONF_AES_SECRETKEY, Constants.DEFAULT_AES_SECRETKEY);
        initCipher(key);

        // For Office Cloud, Tenant Id is not Valid
        if (config.getProperty("tenant.name.as.user.home") != null)
            tenantNameAsUserHome = true;

        LOG.info("init KeystoneManager by url:{}, user:{}, pass:{}",
                authUrl, userName, passWord);
        keystoneManager = KeystoneManager.init(authUrl, userName, passWord);
        try {
            keystoneManager.authService();
        } catch (InterruptedException ie) {
            throw new ServletException("Failed to init KeystoneManager");
        }
    }

    private void initCipher(String key) throws ServletException {
        try {
            SecretKey secretKey = new SecretKeySpec(key.getBytes(), Constants.ENCRYPT_ALGO);
            cipher = Cipher.getInstance(Constants.ENCRYPT_ALGO);
            cipher.init(Cipher.DECRYPT_MODE, secretKey);
        } catch (Exception e) {
            throw new ServletException("Failed to init AES cipher");
        }
    }

    /**
     * @param encryptedString
     * @return
     */
    private String decrypt(String encryptedString) {
        try {
            return new String(cipher.doFinal(new BASE64Decoder().decodeBuffer(encryptedString)));
        } catch (Exception e) {
            LOG.error("Failed to decrypt " + encryptedString, e);
        }
        return null;
    }

    @Override
    public void destroy() {
    }

    @Override
    public String getType() {
        return TYPE;
    }


    @Override
    public boolean managementOperation(AuthenticationToken token,
                                       HttpServletRequest request,
                                       HttpServletResponse response)
            throws IOException, AuthenticationException {
        return true;
    }

    private String getUserName(HttpServletRequest request) {
        String token;
        if (decryptToken(request)) {
            token = request.getParameter(HttpFSFileSystem.X_AUTH_TOKEN_PARAM);
            if (token == null) {
                LOG.warn("x.auth.token not found in request params {}", request.getRequestURI());
                return null;
            }
            token = decrypt(token);
        } else {
            token = request.getHeader(Constants.HEADER_KEYSTONE);
            if (token == null) {
                LOG.warn("Auth-Token Header not in request {}", request.getRequestURI());
                return null;
            }
        }
        TokenCredential tc = keystoneManager.authClient(token);
        return tc == null ? null : (tenantNameAsUserHome ? tc.getTenantName() : tc.getTenantId());
    }

    private boolean decryptToken(HttpServletRequest request) {
        String op = request.getParameter(HttpFSFileSystem.OP_PARAM);
        return op != null && (op.toLowerCase().equals("open") || op.toLowerCase().equals("create"));
    }

    @Override
    public AuthenticationToken authenticate(HttpServletRequest request, HttpServletResponse response)
            throws IOException, AuthenticationException {
        AuthenticationToken token;
        String userName = getUserName(request);
        if (userName == null) {
            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
            token = null;
        } else {
            token = new AuthenticationToken(userName, userName, getType());
        }
        return token;
    }
}
