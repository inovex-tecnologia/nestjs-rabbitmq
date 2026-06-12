# @inovex.tecnologia/nestjs-rabbitmq

Integração RabbitMQ para NestJS:
uma `Connection` **por vhost** (estilo `@VHostX`), um publisher com *publisher confirm*
e um **`RabbitMQBaseListener`** abstrato que tira todo o boilerplate de consumir filas.

```bash
pnpm add @inovex.tecnologia/nestjs-rabbitmq amqplib
```

`@nestjs/common`, `@nestjs/config`, `amqplib` e `reflect-metadata` são **peer dependencies**.

---

## Conceitos

| Peça | Papel |
|---|---|---|
| `RabbitConnectionManager` | 1 conexão long-lived por vhost, com reconexão automática |
| `RabbitMQModule` |  cria 1 provider por vhost |
| `RabbitMQBaseListener` |  base de consumidor: topologia + DLQ + prefetch + ack automático |
| `publish()` |  publica persistente com confirm do broker |
| `collectConnections()` |  lê conexões do ambiente |

---

## 1. Registrar conexões por vhost

```ts
// configuration.ts — só este arquivo lê process.env
import { collectConnections } from '@inovex.tecnologia/nestjs-rabbitmq';

export default () => ({
    rabbitmq: { connections: collectConnections({ primaryVhost: 'stripe' }) },
});
```

```ts
// app.module.ts
import { RabbitMQModule } from '@inovex.tecnologia/nestjs-rabbitmq';

@Module({
    imports: [
        ConfigModule.forRoot({ load: [configuration] }),
        // lê config.get('rabbitmq.connections.<vhost>')
        RabbitMQModule.register(['stripe']),
    ],
})
export class AppModule {}
```

Variáveis de ambiente aceitas (estilo bloco **ou** URL):

```bash
# Bloco (recomendado, tem prioridade)
RABBITMQ__STRIPE__HOST=rabbit.internal
RABBITMQ__STRIPE__PORT=5672
RABBITMQ__STRIPE__VHOST=stripe
RABBITMQ__STRIPE__USERNAME=app
RABBITMQ__STRIPE__PASSWORD=secret
# opcionais: RABBITMQ__STRIPE__PROTOCOL=amqps  RABBITMQ__STRIPE__HEARTBEAT=15

# URL crua
RABBITMQ_URL=amqp://app:secret@rabbit.internal:5672/stripe   # -> primaryVhost
RABBITMQ_URL__B2B=amqp://...                                  # -> vhost "b2b"
```

### Decorator `@VHostX`

```ts
import { createVhostInject } from '@inovex.tecnologia/nestjs-rabbitmq';
export const VHostStripe = createVhostInject('stripe');
```

---

## 2. Publicar

```ts
import { publish, exchange } from '@inovex.tecnologia/nestjs-rabbitmq';

@Injectable()
export class EventPublisher {
    constructor(@VHostStripe() private readonly rabbit: RabbitConnectionManager) {}

    async send(event: { id: string; type: string }) {
        const conn = await this.rabbit.get();
        await publish(conn, exchange('stripe.events', event.type), { event }, {
            messageId: event.id, // o consumidor deduplica por ele
            type: event.type,
        });
    }
}
```

`publish()` usa um *confirm channel* e só resolve após o `waitForConfirms()` do broker.

---

## 3. Consumir — `RabbitMQBaseListener`

```ts
import { RabbitMQBaseListener, type ListenerOptions, type MessageContext } from '@inovex.tecnologia/nestjs-rabbitmq';

@Injectable()
export class StripeEventsConsumer extends RabbitMQBaseListener {
    constructor(
        @VHostStripe() rabbit: RabbitConnectionManager,
        private readonly processor: WebhookProcessorService,
    ) {
        super(rabbit);
    }

    protected options(): ListenerOptions {
        return {
            queue: 'billing.stripe-events',
            prefetch: 10,
            bindings: [{
                exchange: 'stripe.events',
                type: 'topic',
                routingKeys: ['customer.subscription.*', 'payment_intent.*', 'charge.refunded'],
            }],
            // DLQ "billing.stripe-events.dlq" criada automaticamente
        };
    }

    protected async handle(ctx: MessageContext): Promise<void> {
        const { event } = ctx.json<{ event: Stripe.Event }>();
        if (!event?.id) return ctx.nack(); // -> DLQ
        await this.processor.process(event); // ack automático no sucesso
    }
}
```

O base, a cada (re)conexão: declara exchange/fila/bindings/DLQ, aplica `prefetch`,
abre o `consume` com ack manual, faz **ack automático** quando `handle()` resolve e
**nack → DLQ** quando lança. Chame `ctx.ack()`/`ctx.nack()` para controlar manualmente.

### `ListenerOptions`

| Campo | Default | Descrição |
|---|---|---|
| `queue` | — | fila consumida (obrigatório) |
| `prefetch` | `10` | `basicQos` (mín. 1) |
| `assert` | `true` | declara topologia; `false` só consome fila existente |
| `bindings` | `[]` | exchange + routing keys (`['#']` = tudo) |
| `deadLetter` | `${queue}.dlq` | `false` desativa |
| `requeueOnError` | `false` | reenfileira em erro em vez de mandar pra DLQ |

---

## Build

```bash
pnpm install
pnpm build      # tsc -> dist/
```

## Publicar

Automático: a cada **push na `main`**, o workflow (`.github/workflows/publish.yml`)
publica **somente se a versão do `package.json` ainda não existir no npm**. Push de
README/refactor não republica nada — pra lançar, basta dar bump na versão:

```bash
npm version patch        # 0.1.0 -> 0.1.1 (faz commit + tag)
git push --follow-tags   # push na main dispara o publish da nova versão
```

Usa **npm Trusted Publishing (OIDC)** — sem `NPM_TOKEN` armazenado — e anexa
**provenance** automaticamente.

> Primeira publicação: o Trusted Publisher do npm precisa do pacote já existindo.
> Faça a `0.1.0` uma vez manualmente (`npm publish`), depois configure o Trusted
> Publisher no npmjs.com e deixe o restante por conta do workflow.

## Licença

[MIT](LICENSE) © Inovex Tecnologia
