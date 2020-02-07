import com.google.common.annotations.VisibleForTesting;
import io.grpc.*;

/**
 * A interceptor to handle server header.
 */
public class ClientIdInterceptor implements ServerInterceptor {

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            final Metadata requestHeaders,
            ServerCallHandler<ReqT, RespT> next) {
        System.out.println("header received from client:" + requestHeaders);
        Context newContext = Context.current().withValue(Constants.CLIENT_ID_KEY, requestHeaders.get(Constants.CLIENT_ID_KEY_FROM_CLIENT));

        return Contexts.interceptCall(newContext, call, requestHeaders, next);
    }
}