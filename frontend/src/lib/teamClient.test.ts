import { afterEach, describe, expect, it, vi } from 'vitest';
import { createClient } from './client';
import type { ApiKey, ParseableClient, Privilege } from './types';

afterEach(() => vi.unstubAllGlobals());
const json = (value: unknown, status = 200) => new Response(JSON.stringify(value), { status });
const actions: Privilege[] = [{ privilege: 'reader', resource: { stream: 'web_logs' } }];
const key: ApiKey = {
  keyId: 'key/id',
  keyName: 'ingestion',
  apiKey: '****1234',
  roles: ['analysts'],
  createdBy: 'admin',
  createdAt: '2026-10-08T12:00:00Z',
  modifiedAt: '2026-10-08T12:00:00Z',
};
const sources = {
  roles: { analysts: { actions, roleType: 'user' } },
  group_roles: {},
  oidc: {
    issuer: 'https://issuer.example',
    legacy: false,
    groups: ['analysts'],
    manualRoles: [],
    providerRoles: ['analysts'],
    defaultRole: null,
  },
};
type Case = {
  name: string;
  run: (client: ParseableClient, signal?: AbortSignal) => Promise<unknown>;
  url: string;
  method?: string;
  body?: unknown;
  reply?: unknown;
  text?: string;
};
const cases: Case[] = [
  { name: 'about', run: (c, s) => c.about(s), url: '/api/v1/about', reply: {} },
  { name: 'users', run: (c, s) => c.listUsers(s), url: '/api/v1/users', reply: [] },
  {
    name: 'create user',
    run: (c) => c.createUser('new.user', ['analysts']),
    url: '/api/v1/user/new.user',
    method: 'POST',
    body: ['analysts'],
    text: 'one-time-password',
  },
  {
    name: 'delete user',
    run: (c) => c.deleteUser('issuer/sub'),
    url: '/api/v1/user/issuer%2Fsub',
    method: 'DELETE',
  },
  {
    name: 'add user roles',
    run: (c) => c.addUserRoles('issuer/sub', ['analysts']),
    url: '/api/v1/user/issuer%2Fsub/role/add',
    method: 'PATCH',
    body: ['analysts'],
  },
  {
    name: 'remove user roles',
    run: (c) => c.removeUserRoles('issuer/sub', ['analysts']),
    url: '/api/v1/user/issuer%2Fsub/role/remove',
    method: 'PATCH',
    body: ['analysts'],
  },
  {
    name: 'reset password',
    run: (c) => c.resetPassword('issuer/sub'),
    url: '/api/v1/user/issuer%2Fsub/generate-new-password',
    method: 'POST',
    text: 'reset-one-time-password',
  },
  {
    name: 'role sources',
    run: (c, s) => c.userRoleSources('https://issuer/sub @', s),
    url: '/api/v1/user/https%3A%2F%2Fissuer%2Fsub%20%40/role',
    reply: sources,
  },
  {
    name: 'roles',
    run: (c, s) => c.listRoles(s),
    url: '/api/v1/roles',
    reply: { analysts: actions },
  },
  {
    name: 'put role',
    run: (c) => c.putRole('team reader', actions),
    url: '/api/v1/role/team%20reader',
    method: 'PUT',
    body: actions,
  },
  {
    name: 'delete role',
    run: (c) => c.deleteRole('team reader'),
    url: '/api/v1/role/team%20reader',
    method: 'DELETE',
  },
  {
    name: 'default role',
    run: (c, s) => c.defaultRole(s),
    url: '/api/v1/role/default',
    reply: null,
  },
  {
    name: 'set default',
    run: (c) => c.setDefaultRole('analysts'),
    url: '/api/v1/role/default',
    method: 'PUT',
    body: 'analysts',
  },
  {
    name: 'clear default',
    run: (c) => c.clearDefaultRole(),
    url: '/api/v1/role/default',
    method: 'DELETE',
  },
  { name: 'API keys', run: (c, s) => c.listApiKeys(s), url: '/api/prism/v1/apikeys', reply: [key] },
  {
    name: 'create API key',
    run: (c) => c.createApiKey('ingestion', ['analysts']),
    url: '/api/prism/v1/apikeys',
    method: 'POST',
    body: { keyName: 'ingestion', roles: ['analysts'] },
    reply: { ...key, apiKey: 'full-test-key' },
  },
  {
    name: 'create API key with TTL 1800',
    run: (c) => c.createApiKey('ingestion', ['analysts'], 1800),
    url: '/api/prism/v1/apikeys',
    method: 'POST',
    body: { keyName: 'ingestion', roles: ['analysts'], ttlSeconds: 1800 },
    reply: { ...key, expiresAt: '2026-10-08T12:30:00Z' },
  },
  {
    name: 'create API key with TTL null',
    run: (c) => c.createApiKey('ingestion', ['analysts'], null),
    url: '/api/prism/v1/apikeys',
    method: 'POST',
    body: { keyName: 'ingestion', roles: ['analysts'], ttlSeconds: null },
    reply: { ...key, expiresAt: null },
  },
  {
    name: 'create API key with TTL 7776000',
    run: (c) => c.createApiKey('ingestion', ['analysts'], 7776000),
    url: '/api/prism/v1/apikeys',
    method: 'POST',
    body: { keyName: 'ingestion', roles: ['analysts'], ttlSeconds: 7776000 },
    reply: { ...key, expiresAt: '2027-01-06T12:00:00Z' },
  },
  {
    name: 'delete API key',
    run: (c) => c.deleteApiKey('key/id'),
    url: '/api/prism/v1/apikeys/key%2Fid',
    method: 'DELETE',
  },
];

describe('Team HTTP contracts', () => {
  it.each(cases)('$name uses the backend path, cookie session, method and body', async (entry) => {
    const fetch = vi
      .fn()
      .mockResolvedValue(
        entry.text !== undefined
          ? new Response(entry.text, { headers: { 'Content-Type': 'text/plain' } })
          : 'reply' in entry
            ? json(entry.reply)
            : new Response(null),
      );
    vi.stubGlobal('fetch', fetch);
    const signal = new AbortController().signal;
    const result = await entry.run(createClient({ mode: 'live' }), signal);
    const [url, init] = fetch.mock.calls[0];
    expect(url).toBe(entry.url);
    expect(init.credentials).toBe('include');
    expect(init.method).toBe(entry.method);
    if (entry.method === undefined) expect(init.signal).toBe(signal);
    if ('body' in entry) {
      expect(init.headers).toEqual({ 'Content-Type': 'application/json' });
      expect(JSON.parse(init.body)).toEqual(entry.body);
    } else expect(init.body).toBeUndefined();
    if (entry.text !== undefined) expect(result).toBe(entry.text);
  });

  it.each(cases)('$name calls the 401 hook and keeps 403 as an error', async (entry) => {
    const onUnauthorized = vi.fn();
    vi.stubGlobal(
      'fetch',
      vi
        .fn()
        .mockResolvedValueOnce(new Response('Session expired', { status: 401 }))
        .mockResolvedValueOnce(new Response('Permission denied', { status: 403 })),
    );
    const client = createClient({ mode: 'live', onUnauthorized });
    await expect(entry.run(client)).rejects.toMatchObject({
      status: 401,
      message: 'Session expired',
    });
    expect(onUnauthorized).toHaveBeenCalledTimes(1);
    await expect(entry.run(client)).rejects.toMatchObject({
      status: 403,
      message: 'Permission denied',
    });
    expect(onUnauthorized).toHaveBeenCalledTimes(1);
  });

  it.each([
    [{ non_existent_roles: ['a', 'b'] }, 'Roles do not exist: a, b'],
    [{ roles_not_assigned: ['a', 'b'] }, 'Roles are not assigned: a, b'],
  ])('renders structured role errors even with text/plain content type', async (body, message) => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(
        new Response(JSON.stringify(body), {
          status: 400,
          headers: { 'Content-Type': 'text/plain' },
        }),
      ),
    );
    await expect(
      createClient({ mode: 'live' }).removeUserRoles('analyst', ['a']),
    ).rejects.toMatchObject({ status: 400, message, detail: body });
  });

  it.each([
    'Cannot call this API for root admin user',
    'RolesManagedByOidc',
    'Clear or change the default OIDC role before deleting this role.',
  ])('preserves the server message: %s', async (message) => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(message, { status: 400 })));
    await expect(createClient({ mode: 'live' }).deleteRole('test-role')).rejects.toMatchObject({
      status: 400,
      message,
    });
  });

  it('accepts both group field spellings and nullable profile fields, including the root user', async () => {
    const user = {
      id: 'admin',
      username: 'admin',
      method: 'native',
      email: null,
      picture: null,
      roles: { 'super-admin': [{ privilege: 'superadmin' }] },
    };
    vi.stubGlobal(
      'fetch',
      vi
        .fn()
        .mockResolvedValueOnce(
          json([{ ...user, group_roles: { ops: { analysts: actions } }, user_groups: ['ops'] }]),
        )
        .mockResolvedValueOnce(json([{ ...user, groupRoles: {}, userGroups: [] }])),
    );
    const client = createClient({ mode: 'live' });
    expect((await client.listUsers())[0]).toMatchObject({
      groupRoles: { ops: { analysts: actions } },
      userGroups: ['ops'],
      email: undefined,
    });
    expect((await client.listUsers())[0].groupRoles).toEqual({});
  });

  it('normalizes provenance without discarding legacy or nullable fields', async () => {
    vi.stubGlobal(
      'fetch',
      vi
        .fn()
        .mockResolvedValueOnce(json(sources))
        .mockResolvedValueOnce(
          json({
            roles: {},
            groupRoles: {},
            oidc: {
              issuer: null,
              legacy: true,
              groups: null,
              providerRoles: null,
              manualRoles: null,
              defaultRole: null,
            },
          }),
        ),
    );
    const client = createClient({ mode: 'live' });
    expect(await client.userRoleSources('issuer/sub')).toEqual({ ...sources, groupRoles: {} });
    expect((await client.userRoleSources('legacy')).oidc).toMatchObject({
      legacy: true,
      manualRoles: null,
    });
  });

  it('tolerates older about responses without inventing capabilities', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(json({ oidcActive: true })));
    expect(await createClient({ mode: 'live' }).about()).toEqual({
      oidcActive: true,
      capabilities: {
        oidcRoleMapping: false,
        oidcRoleSync: false,
        promql: false,
        promqlAlerts: false,
        promqlDashboard: false,
        promqlMetadata: false,
      },
    });
  });

  const malformedCases: Array<{
    run: (client: ParseableClient) => Promise<unknown>;
    reply: unknown;
  }> = [
    { run: (c) => c.about(), reply: { capabilities: { oidcRoleMapping: 'yes' } } },
    {
      run: (c) => c.listUsers(),
      reply: [
        { id: 'u', username: 'u', method: 'other', roles: {}, group_roles: {}, user_groups: [] },
      ],
    },
    {
      run: (c) => c.listUsers(),
      reply: [
        {
          id: 'u',
          username: 'u',
          method: 'native',
          roles: { r: [{ privilege: 'reader', resource: { stream: 7 } }] },
          groupRoles: {},
          userGroups: [],
        },
      ],
    },
    { run: (c) => c.listRoles(), reply: { readers: { actions } } },
    { run: (c) => c.listRoles(), reply: { readers: [{ privilege: ['reader'] }] } },
    { run: (c) => c.listRoles(), reply: { readers: [{ privilege: 'superadmin' }] } },
    {
      run: (c) => c.userRoleSources('u'),
      reply: { ...sources, oidc: { ...sources.oidc, manualRoles: ['ok', 42] } },
    },
    { run: (c) => c.defaultRole(), reply: { role: 'analysts' } },
    { run: (c) => c.listApiKeys(), reply: [{ ...key, roles: 'analysts' }] },
    { run: (c) => c.createApiKey('key', []), reply: { ...key, createdAt: 'invalid' } },
    ...[42, 'invalid', '', '2026-10-08', '2026-10-08T12:00:00+02:00'].map((expiresAt) => ({
      run: (c: ParseableClient) => c.listApiKeys(),
      reply: [{ ...key, expiresAt }],
    })),
  ];
  it.each(malformedCases)(
    'rejects malformed successful Team payload %#',
    async ({ run, reply }) => {
      vi.stubGlobal('fetch', vi.fn().mockResolvedValue(json(reply)));
      await expect(run(createClient({ mode: 'live' }))).rejects.toMatchObject({
        status: 502,
        message: expect.stringContaining('Unexpected response'),
      });
    },
  );
  it('rejects an empty password response', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('')));
    await expect(
      createClient({ mode: 'live' }).createUser('analyst', ['analysts']),
    ).rejects.toMatchObject({ status: 502 });
  });
  it('never mistakes a role named default for the OIDC configuration endpoint', async () => {
    const fetch = vi.fn();
    vi.stubGlobal('fetch', fetch);
    const client = createClient({ mode: 'live' });
    await expect(client.putRole('default', [])).rejects.toThrow('default OIDC role endpoint');
    await expect(client.deleteRole('DEFAULT')).rejects.toThrow('default OIDC role endpoint');
    expect(fetch).not.toHaveBeenCalled();
  });
});
