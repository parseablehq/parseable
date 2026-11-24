import { describe, expect, it } from 'vitest';
import { createDemoClient } from './demo';

describe('per-client Team demo', () => {
  it('includes each privilege, OIDC capabilities and a masked key inventory', async () => {
    const client = createDemoClient();
    expect(await client.about()).toEqual({
      oidcActive: true,
      capabilities: {
        oidcRoleMapping: true,
        oidcRoleSync: true,
        promql: true,
        promqlAlerts: true,
        promqlDashboard: true,
        promqlMetadata: true,
      },
    });
    expect(
      new Set(
        Object.values(await client.listRoles())
          .flat()
          .map((action) => action.privilege),
      ),
    ).toEqual(new Set(['admin', 'editor', 'writer', 'reader', 'ingestor']));
    const users = await client.listUsers();
    expect(users.find((user) => user.username === 'admin')).toMatchObject({
      method: 'native',
      roles: { 'super-admin': [{ privilege: 'superadmin' }] },
    });
    expect(users.find((user) => user.username === 'analyst')?.groupRoles.analytics).toHaveProperty(
      'auditors',
    );
    expect((await client.listApiKeys()).every((key) => key.apiKey.startsWith('****'))).toBe(true);
  });
  it('replaces full role arrays, permits empty roles and prevents deletion while in use', async () => {
    const client = createDemoClient();
    await client.putRole('new-role', [{ privilege: 'reader', resource: { stream: 'api_logs' } }]);
    await client.putRole('new-role', []);
    expect((await client.listRoles())['new-role']).toEqual([]);
    await client.setDefaultRole('new-role');
    await expect(client.deleteRole('new-role')).rejects.toThrow(
      'Clear or change the default OIDC role',
    );
    await client.clearDefaultRole();
    await client.deleteRole('new-role');
    expect(await client.listRoles()).not.toHaveProperty('new-role');
    await expect(client.deleteRole('analysts')).rejects.toThrow('assigned to an existing user');
  });
  it('creates, assigns, removes, resets and deletes native users without leaking fixture references', async () => {
    const client = createDemoClient();
    expect(await client.createUser('test-user', ['analysts'])).toMatch(/^demo-password-/);
    await client.addUserRoles('test-user', ['editors']);
    await client.removeUserRoles('test-user', ['analysts']);
    expect((await client.userRoleSources('test-user')).roles).toHaveProperty('editors');
    expect(await client.resetPassword('test-user')).toMatch(/^demo-password-/);
    const returned = await client.listRoles();
    returned.editors.length = 0;
    expect((await client.listRoles()).editors).toHaveLength(1);
    await client.deleteUser('test-user');
    expect((await client.listUsers()).some((user) => user.username === 'test-user')).toBe(false);
    expect(
      (await createDemoClient().listUsers()).some((user) => user.username === 'test-user'),
    ).toBe(false);
  });
  it('validates names, duplicate users, unknown roles and missing assignments', async () => {
    const client = createDemoClient();
    await expect(client.putRole('default', [])).rejects.toThrow('default OIDC role endpoint');
    await expect(client.putRole('superadmin', [])).rejects.toThrow('reserved');
    await expect(client.createUser('invalid..name', ['analysts'])).rejects.toThrow('consecutive');
    await expect(client.createUser('analyst', ['analysts'])).rejects.toThrow('already exists');
    await expect(client.createUser('new-user', [])).rejects.toThrow('without a role');
    await expect(client.createUser('new-user', ['missing'])).rejects.toThrow(
      'Roles do not exist: missing',
    );
    await expect(client.removeUserRoles('analyst', ['editors'])).rejects.toThrow(
      'Roles are not assigned: editors',
    );
    await expect(client.setDefaultRole('missing')).rejects.toThrow('existing role');
  });
  it('rejects all root-admin mutations with the server status and message', async () => {
    const client = createDemoClient();
    const users = await client.listUsers();
    const mutations = [
      () => client.createUser('admin', ['analysts']),
      () => client.deleteUser('admin'),
      () => client.resetPassword('admin'),
      () => client.addUserRoles('admin', ['analysts']),
      () => client.removeUserRoles('admin', ['super-admin']),
    ];
    for (const mutate of mutations) {
      await expect(mutate()).rejects.toMatchObject({
        status: 400,
        message: 'Cannot call this API for root admin user',
      });
    }
    expect(await client.listUsers()).toEqual(users);
  });
  describe.each([
    { assigned: ['super-admin'] },
    { assigned: ['analysts', 'super-admin'] },
    { assigned: ['super-admin', 'analysts'] },
  ])('protected role grants: $assigned', ({ assigned }) => {
    it('rejects API-key creation without changing the key inventory', async () => {
      const client = createDemoClient();
      const keys = await client.listApiKeys();
      await expect(client.createApiKey('protected-key', assigned)).rejects.toMatchObject({
        status: 400,
        message: 'Role is protected',
      });
      expect(await client.listApiKeys()).toEqual(keys);
    });
    it('rejects user creation without changing users', async () => {
      const client = createDemoClient();
      const users = await client.listUsers();
      await expect(client.createUser('protected-user', assigned)).rejects.toMatchObject({
        status: 400,
        message: 'Role is protected',
      });
      expect(await client.listUsers()).toEqual(users);
    });
    it.each(['analyst', 'https://issuer.example/sso-user'])(
      'rejects role additions to %s without changing role sources',
      async (id) => {
        const client = createDemoClient();
        const sources = await client.userRoleSources(id);
        await expect(client.addUserRoles(id, assigned)).rejects.toMatchObject({
          status: 400,
          message: 'Role is protected',
        });
        expect(await client.userRoleSources(id)).toEqual(sources);
      },
    );
  });
  it.each(['constructor', 'toString', '__proto__'])(
    'rejects the inherited property name %s as an unknown role',
    async (name) => {
      const client = createDemoClient();
      const unknown = { status: 400, message: `Roles do not exist: ${name}` };
      await expect(client.createApiKey('inherited-key', [name])).rejects.toMatchObject(unknown);
      await expect(client.createUser('inherited-user', [name])).rejects.toMatchObject(unknown);
      await expect(client.addUserRoles('analyst', [name])).rejects.toMatchObject(unknown);
    },
  );
  // Like the backend: API keys and user creation report unknown roles first,
  // while role additions report protected roles first.
  it('orders unknown and protected role errors like the backend', async () => {
    const client = createDemoClient();
    const assigned = ['nope', 'super-admin'];
    const unknown = { status: 400, message: 'Roles do not exist: nope' };
    await expect(client.createApiKey('mixed-key', assigned)).rejects.toMatchObject(unknown);
    await expect(client.createUser('mixed-user', assigned)).rejects.toMatchObject(unknown);
    await expect(client.addUserRoles('analyst', assigned)).rejects.toMatchObject({
      status: 400,
      message: 'Role is protected',
    });
  });
  it('allows ordinary admin roles for API keys and user grants', async () => {
    const client = createDemoClient();
    expect((await client.createApiKey('admin-key', ['administrators'])).roles).toEqual([
      'administrators',
    ]);
    await client.createUser('admin-user', ['administrators']);
    await client.addUserRoles('analyst', ['administrators']);
    for (const id of ['admin-user', 'analyst']) {
      expect((await client.userRoleSources(id)).roles.administrators).toEqual({
        actions: [{ privilege: 'admin' }],
        roleType: 'user',
      });
    }
  });
  it('preserves provider overlap, removes manual grants and reconciles the fallback', async () => {
    const client = createDemoClient();
    const oauth = (await client.listUsers()).find((user) => user.method === 'oauth')!;
    await expect(client.removeUserRoles(oauth.id, ['provider-readers'])).rejects.toThrow(
      'cannot be removed as manual grants',
    );
    await client.addUserRoles(oauth.id, ['provider-readers']);
    await client.removeUserRoles(oauth.id, ['provider-readers', 'auditors']);
    const sources = await client.userRoleSources(oauth.id);
    expect(sources.oidc?.manualRoles).toEqual([]);
    expect(Object.keys(sources.roles)).toEqual(['provider-readers']);
    await client.setDefaultRole('analysts');
    expect((await client.userRoleSources(oauth.id)).oidc?.defaultRole).toBe('analysts');
    await client.clearDefaultRole();
    expect((await client.userRoleSources(oauth.id)).oidc?.defaultRole).toBeNull();
    await expect(client.resetPassword(oauth.id)).rejects.toMatchObject({ status: 404 });
  });
  it('creates, retrieves and deletes keys while listing only masked values', async () => {
    const client = createDemoClient();
    const key = await client.createApiKey('test-key', ['editors']);
    expect(key.keyId).toMatch(/^[0-7][0-9A-HJKMNP-TV-Z]{25}$/);
    expect((await client.listApiKeys()).find((item) => item.keyId === key.keyId)?.apiKey).toBe(
      `****${key.apiKey.slice(-4)}`,
    );
    await expect(client.createApiKey('test-key', [])).rejects.toMatchObject({ status: 409 });
    await expect(client.createApiKey('unknown-role-key', ['missing'])).rejects.toThrow(
      'Roles do not exist',
    );
    await client.deleteApiKey(key.keyId);
    await expect(client.deleteApiKey(key.keyId)).rejects.toMatchObject({ status: 404 });
    expect(await client.listApiKeys()).toHaveLength(2);
  });
  it.each([1800, 7776000])(
    'computes a TTL of %i seconds exactly and preserves native users',
    async (ttlSeconds) => {
      const client = createDemoClient();
      const users = await client.listUsers();
      const key = await client.createApiKey('finite-key', ['analysts'], ttlSeconds);
      expect(Date.parse(key.expiresAt!) - Date.parse(key.createdAt)).toBe(ttlSeconds * 1000);
      expect((await client.listApiKeys()).find((item) => item.keyId === key.keyId)?.expiresAt).toBe(
        key.expiresAt,
      );
      expect(await client.listUsers()).toEqual(users);
    },
  );
  it.each([undefined, null])('creates nonexpiring keys for TTL %s', async (ttlSeconds) => {
    const key = await createDemoClient().createApiKey('permanent-key', ['analysts'], ttlSeconds);
    expect(key.expiresAt).toBeNull();
  });
  it.each([0, 1799, 7776001, -1, 1800.5, NaN, Infinity])(
    'rejects invalid TTL %s without creating a key',
    async (ttlSeconds) => {
      const client = createDemoClient();
      await expect(
        client.createApiKey('invalid-key', ['analysts'], ttlSeconds),
      ).rejects.toMatchObject({ status: 400 });
      expect(await client.listApiKeys()).toHaveLength(2);
    },
  );
  it('honors read cancellation without sharing mutations with another client', async () => {
    const client = createDemoClient();
    const other = createDemoClient();
    await client.putRole('local-role', []);
    expect(await other.listRoles()).not.toHaveProperty('local-role');
    const controller = new AbortController();
    controller.abort();
    await expect(client.userRoleSources('unused', controller.signal)).rejects.toMatchObject({
      name: 'AbortError',
    });
    await expect(client.about(controller.signal)).rejects.toMatchObject({ name: 'AbortError' });
  });
  it('applies the default only when an OAuth user has no provider or manual grants', async () => {
    const client = createDemoClient();
    const fallback = (await client.listUsers()).find((user) => user.username === 'sso.fallback')!;
    expect(Object.keys((await client.userRoleSources(fallback.id)).roles)).toEqual(['observers']);
    await client.setDefaultRole('analysts');
    expect(Object.keys((await client.userRoleSources(fallback.id)).roles)).toEqual(['analysts']);
    await client.addUserRoles(fallback.id, ['editors']);
    await client.setDefaultRole('ingestors');
    expect(Object.keys((await client.userRoleSources(fallback.id)).roles)).toEqual(['editors']);
    await client.removeUserRoles(fallback.id, ['editors']);
    expect(Object.keys((await client.userRoleSources(fallback.id)).roles)).toEqual(['ingestors']);
    await client.clearDefaultRole();
    expect((await client.userRoleSources(fallback.id)).roles).toEqual({});
  });
});
