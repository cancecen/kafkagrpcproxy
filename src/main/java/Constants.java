import io.grpc.Context;
import io.grpc.Metadata;

public class Constants {
    public static final Metadata.Key<String> CLIENT_ID_KEY_FROM_CLIENT =
            Metadata.Key.of("clientId", Metadata.ASCII_STRING_MARSHALLER);
    public static final Context.Key<String> CLIENT_ID_KEY = Context.key("clientId");
}
