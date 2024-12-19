import NIO
import Logging
import MongoCore
import Foundation

internal struct MongoResponseContext {
    let requestId: Int32
    let result: EventLoopPromise<MongoServerReply>
}

public final class MongoClientContext {
    private var queries = [Int32: MongoResponseContext]()
    private let queriesQueue = DispatchQueue(label: "org.openkitten.mongokitten.core.queries")
    
    internal var serverHandshake: ServerHandshake? {
        didSet {
            if let version = serverHandshake?.maxWireVersion, version.isDeprecated {
                logger.warning("MongoDB server is outdated, please upgrade MongoDB")
            }
        }
    }
    internal var didError = false
    let logger: Logger

    internal func handleReply(_ reply: MongoServerReply) -> Bool {
        let query = queriesQueue.sync {
            return queries.removeValue(forKey: reply.responseTo)
        }
        guard let query else {
            return false
        }
        query.result.succeed(reply)
        return true
    }

    internal func awaitReply(toRequestId requestId: Int32, completing result: EventLoopPromise<MongoServerReply>) {
        queriesQueue.async { [weak self] in
            self?.queries[requestId] = MongoResponseContext(requestId: requestId, result: result)
        }
    }
    
    deinit {
        self.cancelQueries(MongoError(.queryFailure, reason: .connectionClosed))
    }
    
    public func failQuery(byRequestId requestId: Int32, error: Error) {
        let query = queriesQueue.sync {
            return queries.removeValue(forKey: requestId)
        }
        query?.result.fail(error)
    }

    public func cancelQueries(_ error: Error) {
        queriesQueue.sync { [weak self] in
            guard let self else {
                return
            }
            for query in queries.values {
                query.result.fail(error)
            }

            queries = [:]
        }
        
    }

    public init(logger: Logger) {
        self.logger = logger
    }
}

struct MongoClientRequest<Request: MongoRequestMessage> {
    let command: Request
    let namespace: MongoNamespace
    
    init(command: Request, namespace: MongoNamespace) {
        self.command = command
        self.namespace = namespace
    }
}
