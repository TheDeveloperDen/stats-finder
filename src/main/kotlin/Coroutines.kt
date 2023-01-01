import kotlinx.coroutines.*
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.future.await
import net.dv8tion.jda.api.requests.RestAction
import net.dv8tion.jda.api.utils.concurrent.Task
import kotlin.coroutines.coroutineContext
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

suspend fun <T> RestAction<T>.await(): T = submit().await()

suspend fun <T> Task<T>.await(): T = suspendCancellableCoroutine { cont ->
    onSuccess { cont.resume(it) }
    onError { cont.resumeWithException(it) }
    cont.invokeOnCancellation { cancel() }
}


val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())


@OptIn(ExperimentalCoroutinesApi::class, InternalCoroutinesApi::class)
@FlowPreview
fun <T, R> Flow<T>.mapParallel(scope: CoroutineScope, bufferSize: Int = 16, transform: suspend (T) -> R) =
    flow {
        val currentContext = coroutineContext.minusKey(Job) // Jobs are ignored

        coroutineScope {
            val channel = produce(currentContext, capacity = bufferSize) {
                collect { value ->
                    send(scope.async { transform(value) })
                }
            }

            (channel as Job).invokeOnCompletion { if (it is CancellationException && it.cause == null) cancel() }
            for (element in channel) {
                emit(element.await())
            }

            val producer = channel as Job
            if (producer.isCancelled) {
                producer.join()
                throw producer.getCancellationException()
            }
        }
    }