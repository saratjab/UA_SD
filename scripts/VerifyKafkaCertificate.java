import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.cert.CertPath;
import java.security.cert.CertPathValidator;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.PKIXParameters;
import java.security.cert.TrustAnchor;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Validates the Kafka server PKCS#12 chain and exact DNS SANs before startup. */
public final class VerifyKafkaCertificate {
    private static final String SERVER_AUTH = "1.3.6.1.5.5.7.3.1";

    private VerifyKafkaCertificate() {}

    public static void main(String[] args) {
        try {
            if (args.length != 3) throw new IllegalArgumentException();
            String password = System.getenv("KAFKA_TLS_KEYSTORE_PASSWORD");
            if (password == null || password.isEmpty()) throw new IllegalArgumentException();
            verify(args[0], args[1], password.toCharArray(), args[2].split(",", -1));
        } catch (Exception ignored) {
            System.err.println("Kafka TLS certificate chain or exact DNS SAN validation failed");
            System.exit(1);
        }
    }

    private static void verify(String keystorePath, String caPath, char[] password, String[] requiredHosts) throws Exception {
        CertificateFactory factory = CertificateFactory.getInstance("X.509");
        X509Certificate ca;
        try (FileInputStream input = new FileInputStream(caPath)) {
            ca = (X509Certificate) factory.generateCertificate(input);
        }
        ca.checkValidity();
        if (ca.getBasicConstraints() < 0) throw new IllegalArgumentException("Configured trust certificate is not a CA");
        boolean[] caUsage = ca.getKeyUsage();
        if (caUsage != null && (caUsage.length <= 5 || !caUsage[5])) {
            throw new IllegalArgumentException("Configured CA cannot sign certificates");
        }

        KeyStore store = KeyStore.getInstance("PKCS12");
        try (FileInputStream input = new FileInputStream(keystorePath)) {
            store.load(input, password);
        }
        X509Certificate leaf = null;
        List<X509Certificate> chain = null;
        Enumeration<String> aliases = store.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            if (!store.isKeyEntry(alias)) continue;
            Certificate[] certificates = store.getCertificateChain(alias);
            if (certificates == null || certificates.length == 0) continue;
            leaf = (X509Certificate) certificates[0];
            chain = new ArrayList<>();
            for (Certificate certificate : certificates) chain.add((X509Certificate) certificate);
            break;
        }
        if (leaf == null || chain == null) throw new IllegalArgumentException("Keystore has no private-key entry");
        leaf.checkValidity();
        List<String> extendedUsage = leaf.getExtendedKeyUsage();
        if (extendedUsage == null || !extendedUsage.contains(SERVER_AUTH)) {
            throw new IllegalArgumentException("Server certificate lacks serverAuth usage");
        }
        Set<String> dnsSans = new HashSet<>();
        var names = leaf.getSubjectAlternativeNames();
        if (names != null) {
            for (var name : names) {
                if (name.size() >= 2 && Integer.valueOf(2).equals(name.get(0)) && name.get(1) instanceof String) {
                    dnsSans.add((String) name.get(1));
                }
            }
        }
        for (String host : requiredHosts) {
            if (host.isEmpty() || dnsSans.stream().noneMatch(host::equalsIgnoreCase)) {
                throw new IllegalArgumentException("Required hostname is not an exact DNS SAN");
            }
        }

        List<X509Certificate> pathCertificates = new ArrayList<>();
        for (X509Certificate certificate : chain) {
            if (certificate.equals(ca)) break;
            certificate.checkValidity();
            pathCertificates.add(certificate);
        }
        if (pathCertificates.isEmpty()) throw new IllegalArgumentException("Server certificate cannot be its own trust anchor");
        CertPath path = factory.generateCertPath(pathCertificates);
        PKIXParameters parameters = new PKIXParameters(Collections.singleton(new TrustAnchor(ca, null)));
        parameters.setRevocationEnabled(false);
        CertPathValidator.getInstance("PKIX").validate(path, parameters);
    }
}
