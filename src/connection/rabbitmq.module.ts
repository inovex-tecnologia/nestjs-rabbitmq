import { DynamicModule, Global, Module, type Provider } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { RabbitConnectionManager, type RabbitConnectionOptions } from './rabbit-connection';
import type { RabbitConnectionSource } from './connection-source';
import { rabbitToken } from './rabbit.decorators';

/** Token interno do mapa de fontes resolvido por `registerAsync`. */
const SOURCES_TOKEN = 'RABBIT_CONNECTION_SOURCES';

export interface RegisterOptions extends RabbitConnectionOptions {
    /**
     * Caminho no ConfigService onde estao as fontes por vhost.
     * Espera `config.get(`${configPath}.${vhost}`)` -> RabbitConnectionSource.
     * Default: `'rabbitmq.connections'`.
     */
    configPath?: string;
}

export interface RegisterAsyncOptions extends RabbitConnectionOptions {
    /** Nomes de vhost a expor (1 Connection injetavel por nome). */
    vhosts: string[];
    /** Resolve o mapa `{ [vhost]: RabbitConnectionSource }`. */
    useFactory: (
        ...args: unknown[]
    ) => Record<string, RabbitConnectionSource> | Promise<Record<string, RabbitConnectionSource>>;
    /** Dependencias injetadas no `useFactory`. */
    inject?: unknown[];
}

/**
 * Disponibiliza um {@link RabbitConnectionManager} POR VHOST para todo o app — o
 * equivalente em Nest dos varios `@VHostX` do projeto Java.
 *
 *   // Lendo do ConfigService (convencao `rabbitmq.connections.<vhost>`):
 *   imports: [RabbitMQModule.register(['stripe', 'b2b'])]
 *
 *   // Resolvendo as fontes via factory:
 *   imports: [RabbitMQModule.registerAsync({
 *       vhosts: ['stripe'],
 *       inject: [ConfigService],
 *       useFactory: (c: ConfigService) => ({ stripe: c.getOrThrow('RABBITMQ_URL') }),
 *   })]
 */
@Global()
@Module({})
export class RabbitMQModule {
    static register(vhosts: string[], options: RegisterOptions = {}): DynamicModule {
        const configPath = options.configPath ?? 'rabbitmq.connections';
        const connOptions: RabbitConnectionOptions = { reconnectMs: options.reconnectMs };

        const providers: Provider[] = vhosts.map((vhost) => ({
            provide: rabbitToken(vhost),
            inject: [ConfigService],
            useFactory: (config: ConfigService) => {
                const source = config.getOrThrow<RabbitConnectionSource>(`${configPath}.${vhost}`);
                return new RabbitConnectionManager(vhost, source, connOptions);
            },
        }));

        return {
            module: RabbitMQModule,
            providers,
            exports: providers.map((p) => (p as { provide: string }).provide),
        };
    }

    static registerAsync(options: RegisterAsyncOptions): DynamicModule {
        const connOptions: RabbitConnectionOptions = { reconnectMs: options.reconnectMs };

        const sourcesProvider: Provider = {
            provide: SOURCES_TOKEN,
            inject: (options.inject ?? []) as never[],
            useFactory: options.useFactory,
        };

        const connProviders: Provider[] = options.vhosts.map((vhost) => ({
            provide: rabbitToken(vhost),
            inject: [SOURCES_TOKEN],
            useFactory: (sources: Record<string, RabbitConnectionSource>) =>
                new RabbitConnectionManager(vhost, sources[vhost], connOptions),
        }));

        return {
            module: RabbitMQModule,
            providers: [sourcesProvider, ...connProviders],
            exports: connProviders.map((p) => (p as { provide: string }).provide),
        };
    }
}
