package server.interceptors;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import server.constants.Constants;

/** A interceptor to handle server header. */
public class ClientIdInterceptor implements ServerInterceptor {
  private static final Logger logger = LoggerFactory.getLogger(ClientIdInterceptor.class);

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call,
      final Metadata requestHeaders,
      ServerCallHandler<ReqT, RespT> next) {
    logger.info("header received from client:" + requestHeaders);
    Context newContext =
        Context.current()
            .withValue(
                Constants.CLIENT_ID_KEY, requestHeaders.get(Constants.CLIENT_ID_KEY_FROM_CLIENT));

    return Contexts.interceptCall(newContext, call, requestHeaders, next);
  }
}
