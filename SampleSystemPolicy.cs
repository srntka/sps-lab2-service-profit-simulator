using System;
using System.Collections.Generic;
using System.Linq;
using SPS.Abstractions;

namespace SPS.Student;

public sealed class PanasenkoProfitPolicy : ISystemPolicy
{
    private static readonly TimeSpan DeliveryDelay = TimeSpan.FromMilliseconds(2);
    private static readonly TimeSpan CacheTtl = TimeSpan.FromMinutes(1);
    private readonly Dictionary<string, TimeSpan> cacheAccess = new(StringComparer.Ordinal);
    private readonly Dictionary<long, TimeSpan> workerHeartbeat = new();
    private TimeSpan heartbeatInterval = TimeSpan.FromMilliseconds(500);
    private TimeSpan? workerScaleDownSince;
    private TimeSpan? apiScaleDownSince;

    public IRequestPolicy RpcPolicy { get; } = new AdaptiveRpcPolicy();

    // Бюджет обработки (вызывается один раз при поступлении запроса
    // и задает предельное время его жизни
    public TimeSpan? GetProcessingBudget(RequestInfo request, Random random) => request.Kind switch
    {
        RequestKind.Metadata => TimeSpan.FromSeconds(1.5),
        RequestKind.Preview => TimeSpan.FromSeconds(3),
        _ => TimeSpan.FromSeconds(60)
    };

    // Чтение кэша - 1. отдать из кэша (если есь запись, ттл не истек и достаточно свежая
    // 2. присоединиться к идущ загрузке (если другой клиент запросил тот же ключ) только к свежим!
    // 3. начать свою загрузку 
    public CacheReadDecision OnCacheRead(CacheReadContext context, Random random)
    {
        var entry = context.Entry;
        if (entry != null && entry.ExpiresAt > context.Now &&
            context.Now - entry.SourceObservedAt + DeliveryDelay <= context.Request.MaxDataAge)
        {
            cacheAccess[entry.Key] = context.Now;
            return CacheReadDecision.ServeCached();
        }

        
        var joinWindow = TimeSpan.FromMilliseconds(context.Request.Kind == RequestKind.Metadata ? 120 : 250);
        var fetch = context.PendingFetches
            .Where(f => context.Now - f.StartedAt <= joinWindow &&
                context.Now - f.StartedAt + DeliveryDelay <= context.Request.MaxDataAge)
            .OrderByDescending(f => f.StartedAt)
            .FirstOrDefault();
        return fetch == null ? CacheReadDecision.StartFetch() : CacheReadDecision.JoinFetch(fetch.Id);
    }

    // Запись в кэш 
    public CacheWriteDecision OnCacheFill(CacheFillContext context, Random random)
    {
        var value = context.Value;
        if (value.SizeBytes > context.CapacityBytes || context.Entries.Any(entry =>
            entry.Key == value.Key && entry.SourceObservedAt > value.SourceObservedAt))
            return CacheWriteDecision.Skip();

        var otherEntries = context.Entries.Where(entry => entry.Key != value.Key).ToArray();
        long bytesToFree = otherEntries.Sum(entry => entry.SizeBytes) - (context.CapacityBytes - value.SizeBytes);
        var evictions = new List<string>();

        // что убарть из кэша - lru
        foreach (var entry in otherEntries.OrderBy(entry => cacheAccess.GetValueOrDefault(entry.Key, entry.SourceObservedAt)))
        {
            if (bytesToFree <= 0) break;
            evictions.Add(entry.Key);
            bytesToFree -= entry.SizeBytes;
        }
        cacheAccess[value.Key] = context.Now;
        return CacheWriteDecision.Retain(CacheTtl, evictions.ToArray());
    }

    // Выбор API - выбираю самый свободный, распределяю учитывая длину очереди относительно мозности
    public EndpointSelectionDecision? SelectEndpoint(RoutingContext context, Random random)
    {
        var endpoint = context.Endpoints
            .OrderBy(api => (api.OutstandingCalls + 1.0) / (api.Slots * api.Speed))
            .FirstOrDefault();
        return endpoint == null ? null : EndpointSelectionDecision.Assign(endpoint.Id);
    }

    // Здоровье серверов
    public EndpointDecision ReviewEndpoint(EndpointContext context, Random random)
    {
        var endpoint = context.Endpoint;
        if (endpoint.State == InstanceState.Draining) return EndpointDecision.Allow();
        if (endpoint.Role == InstanceRole.Worker)
        {
            // если воркер не присылал хартбит дольше 4 инт - блок
            return endpoint.LastHeartbeat is { } heartbeat && context.Now - heartbeat > heartbeatInterval * 4
                ? EndpointDecision.Block()
                : EndpointDecision.Allow();
        }

        var results = context.RecentResults;
        if (endpoint.Mode == EndpointMode.Probe)
        {
            return !endpoint.ProbeOutstanding && results.LastOrDefault(result => result.IsProbe)?.Outcome == RequestOutcome.Success
                ? EndpointDecision.Allow()
                : EndpointDecision.Probe();
        }
        // если 3 ласт вызова подряд закончились таймаутом или недоступом за лст 2 сек - сервер поломан - проверка

        var latest = results.TakeLast(3).ToArray();
        bool failing = latest.Length == 3 && latest.All(result =>
            context.Now - result.Time <= TimeSpan.FromSeconds(2) &&
            result.Outcome is RequestOutcome.Timeout or RequestOutcome.Unavailable);
        return failing ? EndpointDecision.Probe() : EndpointDecision.Allow();
    }

    // Выбор задания для воркера
    // Приоритет - самый быстрый воркер
    // 
    public JobAssignmentDecision? SelectJobAssignment(JobSchedulingContext context, Random random)
    {
        var worker = context.AvailableWorkers.OrderByDescending(candidate => candidate.Speed).FirstOrDefault();
        if (worker == null) return null;

        JobInfo? bestJob = null;
        double bestScore = 0;
        foreach (var job in context.QueuedJobs)
        {
            double duration = JobSeconds(job, worker.Speed);
            double completionAge = (context.Now - job.Request.CreatedAt).TotalSeconds + duration;
            double chance = Math.Clamp((60 - completionAge) / 45, 0, 1);
            // ожидаемая выручка на секунду работы воркера
            double score = (double)job.Request.Revenue * chance / duration;
            if (completionAge >= 60) return JobAssignmentDecision.Reject(job.Id);
            if (bestJob == null || score > bestScore)
            {
                bestJob = job;
                bestScore = score;
            }
        }
        return bestJob == null ? null : JobAssignmentDecision.Assign(bestJob.Id, worker.Id);
    }

    // Восстановление заданий
    // если вернуть задание в очередь слишком рано - дубль и лишние расходы
    public JobRecoveryDecision ReviewJob(JobRecoveryContext context, Random random)
    {
        var job = context.Job;
        if (job.AssignedAt is not { } assignedAt || job.Attempt >= 3)
            return JobRecoveryDecision.KeepWaiting();

        var lastProgress = job.LastHeartbeat ?? assignedAt;
        bool workerStillReporting = job.WorkerId is { } workerId &&
            workerHeartbeat.TryGetValue(workerId, out var heartbeat) &&
            context.Now - heartbeat <= heartbeatInterval * 2;
        // даем время воркеру на подумать
        var grace = heartbeatInterval * (workerStillReporting ? 4 : 6);
        return context.Now - lastProgress > grace
            ? JobRecoveryDecision.RequeueAfter(TimeSpan.FromMilliseconds(50 + 150 * random.NextDouble()))
            : JobRecoveryDecision.KeepWaiting();
    }

    // Управление мощностью 
    public CapacityPlan ReviewCapacity(CapacityContext context, Random random)
    {
        var settings = context.Settings;
        var apis = context.Instances.Where(endpoint => endpoint.Role == InstanceRole.Api &&
            endpoint.State is InstanceState.Ready or InstanceState.Starting).ToArray();
        var workers = context.Instances.Where(endpoint => endpoint.Role == InstanceRole.Worker &&
            endpoint.State is InstanceState.Ready or InstanceState.Starting).ToArray();

        int desiredApis = Math.Max(1, apis.Length);
        int apiSlots = Math.Max(1, settings.Api.Slots);
        // Если ожидающих вызовов больше, чем слотов, значит, не справляемся, и нужен ещё один API
        int usableApiSlots = apis.Where(api => api.State == InstanceState.Starting || api.Mode == EndpointMode.AllowTraffic)
            .Sum(api => api.Slots);
        if (context.PendingCalls > usableApiSlots)
        {
            apiScaleDownSince = null;
            desiredApis++;
        }
        // Сокращаем если API больше одного и после уменьшения оставшиеся серверы были бы загружены не больше чем наполовину, и так держится достаточно долго
        else if (CanScaleDown(context.Now, desiredApis > 1 && context.PendingCalls <= (desiredApis - 1) * apiSlots / 2,
            settings.Api.StartupDelay, ref apiScaleDownSince)) desiredApis--;

        double horizon = Math.Max(2, settings.Worker.StartupDelay.TotalSeconds + 1);
        double queuedWork = context.QueuedJobs
            .Where(job => job.Request.Revenue > settings.Worker.PricePerSecond * (decimal)(JobSeconds(job, settings.Worker.Speed) / settings.Worker.Slots) &&
                (context.Now - job.Request.CreatedAt).TotalSeconds + horizon + JobSeconds(job, settings.Worker.Speed) < 60)
            .Sum(job => JobSeconds(job, settings.Worker.Speed));
        int workerSlots = Math.Max(1, settings.Worker.Slots);
        int assigned = workers.Sum(worker => worker.AssignedJobs);
        int starting = workers.Count(worker => worker.State == InstanceState.Starting);
        int active = workers.Count(worker => worker.Mode == EndpointMode.AllowTraffic || worker.State == InstanceState.Starting);
        int needed = (int)Math.Ceiling((assigned + queuedWork / horizon) / workerSlots);
        int desiredWorkers = Math.Max(1, workers.Length);
        var idleWorker = workers.Where(worker => worker.State == InstanceState.Ready && worker.AssignedJobs == 0)
            .OrderBy(worker => worker.Mode == EndpointMode.AllowTraffic)
            .ThenBy(worker => worker.Speed)
            .FirstOrDefault();
        long[]? retire = null;
        // Если есть полезная работа, нужно больше, чем активно, и сейчас никто не запускается
        if (queuedWork > 0 && needed > active && starting == 0)
        {
            workerScaleDownSince = null;
            desiredWorkers = Math.Max(desiredWorkers + 1, needed);
        }
        //Если очереди нет, никто не запускается и есть простаивающий воркер, то сокращаем
        else if (CanScaleDown(context.Now, desiredWorkers > 1 && queuedWork == 0 &&
            starting == 0 && idleWorker != null, settings.Worker.StartupDelay, ref workerScaleDownSince) && idleWorker != null)
        {
            desiredWorkers--;
            retire = new[] { idleWorker.Id };
        }

        // Общий лимит
        int units = settings.ComputeUnitLimit; 
        int minimumApis = settings.MaximumApis > 0 && units >= settings.Api.ComputeUnits ? 1 : 0;
        units -= minimumApis * settings.Api.ComputeUnits;
        desiredWorkers = Math.Min(desiredWorkers, Math.Min(settings.MaximumWorkers, units / settings.Worker.ComputeUnits));
        units -= desiredWorkers * settings.Worker.ComputeUnits;
        desiredApis = minimumApis + Math.Min(Math.Max(0, desiredApis - minimumApis),
            Math.Min(settings.MaximumApis - minimumApis, units / settings.Api.ComputeUnits));
        return new CapacityPlan(desiredApis, desiredWorkers, retire);
    }

    //Простой должен длиться непрерывно не менее max(5 с, 2*время запуска сервера)
    private static bool CanScaleDown(TimeSpan now, bool idle, TimeSpan startupDelay, ref TimeSpan? idleSince)
    {
        if (!idle)
        {
            idleSince = null;
            return false;
        }
        idleSince ??= now;
        if (now - idleSince.Value < TimeSpan.FromSeconds(Math.Max(5, startupDelay.TotalSeconds * 2))) return false;
        idleSince = null;
        return true;
    }

    // Оценка времени выполнения: объём работы, делённый на скорость воркера, плюс 0,2 с на накладные расходы
    private static double JobSeconds(JobInfo job, double speed) =>
        Math.Max(0.05, job.Request.EstimatedWorkSeconds / speed + 0.2);

    public void Observe(SystemObservation observation)
    {
        if (observation.Kind == ObservationKind.SimulationStarted && observation.Settings is { } settings)
            heartbeatInterval = settings.HeartbeatInterval;
        if (observation.Kind == ObservationKind.CacheEntryRemoved && observation.Key is { } key)
            cacheAccess.Remove(key);
        if (observation.EndpointId is not { } endpointId) return;
        if (observation.Kind == ObservationKind.WorkerHeartbeatReceived)
            workerHeartbeat[endpointId] = observation.Now;
        if (observation.Kind == ObservationKind.InstanceStopped)
            workerHeartbeat.Remove(endpointId);
    }

    private sealed class AdaptiveRpcPolicy : IRequestPolicy
    {
        private readonly Queue<double> successfulTimes = new();
        private readonly Queue<bool> recentTimeouts = new();
        private TimeSpan timeout = TimeSpan.FromMilliseconds(350);

        public TimeSpan GetTimeout(RequestContext context, Random random) => timeout;

        public RetryDecision OnResult(RequestObservation observation, Random random)
        {
            recentTimeouts.Enqueue(observation.Outcome == RequestOutcome.Timeout);
            if (recentTimeouts.Count > 100) recentTimeouts.Dequeue();
            if (observation.Outcome == RequestOutcome.Success)
            {
                successfulTimes.Enqueue(observation.Elapsed.TotalMilliseconds);
                if (successfulTimes.Count > 100) successfulTimes.Dequeue();
            }

            double timeoutShare = recentTimeouts.Count(timedOut => timedOut) / (double)recentTimeouts.Count;
            if (successfulTimes.Count >= 20)
            {
                var sorted = successfulTimes.OrderBy(time => time).ToArray();
                double target = sorted[(int)Math.Ceiling(sorted.Length * 0.95) - 1] * 1.8;
                if (timeoutShare > 0.10) target = Math.Max(target, timeout.TotalMilliseconds * 1.02);
                timeout = TimeSpan.FromMilliseconds(Math.Clamp(target, 180, 800));
            }

            // One retry at most, and none when timeouts are already frequent.
            return observation.Attempt == 1 && timeoutShare <= 0.20 &&
                observation.Outcome is RequestOutcome.Timeout or RequestOutcome.Rejected or RequestOutcome.Unavailable
                ? RetryDecision.RetryAfter(TimeSpan.FromMilliseconds(20 + 50 * random.NextDouble()))
                : RetryDecision.NoRetry();
        }
    }
}
