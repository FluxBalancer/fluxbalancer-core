Да, здесь можно **вообще не усложнять**. Если ты не хочешь тащить profile-aware latency в feature vector этого use case,
то самый простой и чистый вариант такой: - **в `ChooseNodeUseCase` пока не использовать `request_profile` вообще** -
оставить аргумент в сигнатуре только для совместимости пайплайна - profile использовать **только в `ReplicationPlanner`
для `tau` и adaptive replication** - позже, если захочешь, уже подтянешь profile-aware ranking То есть прямо в этом
классе можно оставить почти всё как есть. ## Самый простой вариант
``` ``` ##
Почему это нормально Потому что у тебя две разные задачи: 1. **Выбрать хороший узел в целом** 2. **Посчитать `tau` и
adaptive replication по релевантной latency-истории** Для второй задачи `request_profile` критичен. Для первой — не
обязательно сразу. То есть можно сделать поэтапно: - сейчас: - `ChooseNodeUseCase` не трогаем по смыслу -
`request_profile` используется только в `ReplicationPlanner` и при записи latency - потом: - если понадобится, добавить
profile-aware ranking --- ## Где `request_profile` реально нужен уже сейчас ### В `ProxyRequestUseCase` Там ты его
строишь: ```python request_profile = build_request_profile( path=request.url.path, query=request.query_params, ) ``` ###
В запись latency обычного запроса
```python await self.metrics_repo.add_latency( node_id=node_id, latency_ms=latency, profile=request_profile, ) ``` ### В
`ReplicationManager` Передаёшь дальше в planner и runner. ### В `ReplicationPlanner` Вот тут он реально нужен:
```python node_samples = await self.metrics_repository.get_latency_samples( node_id=node_id, profile=request_profile, ) if not node_samples: node_samples = await self.metrics_repository.get_latency_samples( node_id=node_id, profile=None, ) ```
И потом именно из этих `node_samples` считаешь `tau`. --- ## То есть ответ на твой вопрос **Никак не использовать в этом
классе — это абсолютно нормальный промежуточный вариант.** Просто оставь параметр:
```python request_profile: str | None = None ``` и не трогай тело метода. Это лучше, чем насильно тащить сюда новую
сложность. --- ## Если хочешь совсем аккуратно Чтобы линтер не ругался на неиспользуемый аргумент, можно так:
```python _ = request_profile ``` в начале `rank_nodes(...)`:
```python async def rank_nodes( self, brs: BRSRequest, request_profile: str | None = None, ) -> list[tuple[str, str, int]]: _ = request_profile balancer_strategy: RankingStrategy = self.decision_policy.resolve_balancer(brs) ... ``` --- ##
Практическая рекомендация Сделай сейчас именно так: - `ChooseNodeUseCase` — без новой логики - `MetricsRepository` —
profile-aware - `ProxyRequestUseCase` — пишет latency с profile - `ReplicationPlanner` — читает latency с profile для
`tau` - `AiohttpReplicationRunner` — пишет latency реплик с profile Этого уже хватит, чтобы `tau` перестал считаться на
мусоре. Следующим сообщением могу показать **минимальный патч только для `ReplicationPlanner`**, без переписывания всего
остального.