# ghosty-tui

Chat de terminal, mínimo y rápido, para hablar con un agente **ghosty** (rust-ghosty
y demás templates de chat web de [easybits](https://www.easybits.cloud)) por SSE.

Tokens en vivo, caja de input fija, una sola sesión. El binario se llama `ghosty`.

```
┌ ghosty · 6a297a67… ───────────────────────┐
│ › hola, ¿qué puedes hacer?                 │
│ ghosty Puedo ayudarte con ventas, gestionar│
│        tus documentos, clientes y pedidos… │
└────────────────────────────────────────────┘
 ○ listo · Enter envía · Esc sale · PgUp/PgDn scroll
┌ mensaje ───────────────────────────────────┐
│ _                                          │
└────────────────────────────────────────────┘
```

## Instalar

Requiere [Rust](https://rustup.rs) (compila desde fuente, ~1 min):

```bash
cargo install --git https://github.com/blissito/ghosty-tui
```

Quedará el binario `ghosty` en tu PATH (`~/.cargo/bin`).

## Usar

```bash
ghosty --agent <agentId> --token <embedToken>
```

`agentId` y `embedToken` salen al crear un agente con el MCP de easybits
(`agent_create({})`) o en `GET /api/v2/agents`. De hecho la API te devuelve el
comando ya armado en el campo `tuiCommand` — solo cópialo y pégalo.

Fallback por entorno:

```bash
export GHOSTY_AGENT=<agentId>
export GHOSTY_TOKEN=<embedToken>
ghosty
```

### Headless (pipes / scripts)

Un turno, tokens a stdout, sin TUI:

```bash
ghosty --agent <id> --token <embedToken> --once "¿qué eres en una frase?"
```

## Teclas

| Tecla | Acción |
|---|---|
| `Enter` | enviar |
| `Esc` / `Ctrl+C` | salir |
| `PgUp` / `PgDn` | scroll del historial |

## Cómo funciona

Habla con el endpoint público `POST /api/v2/agents/{id}/message` de easybits,
autenticando con el `embedToken` como `Bearer`, y consume el stream SSE
(`data: {type:"chunk"|"token"|"error"|"done", value?}`) token por token.

- `src/client.rs` — cliente SSE (`reqwest` + parseo de frames).
- `src/main.rs` — CLI + bucle [ratatui](https://ratatui.rs) con `tokio::select!`
  entre el teclado y el stream.

## Licencia

MIT
