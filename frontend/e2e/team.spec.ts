import { test, expect, type Locator, type Page } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

async function demo(page: Page, path = '/team') {
  await page.addInitScript(() => sessionStorage.setItem('parseable-mode', 'demo'));
  await page.goto(path);
  await expect(page.getByRole('heading', { name: 'Access management' })).toBeVisible();
  await expect(page.getByRole('table')).toBeVisible();
}
const namedRow = (page: Page, name: string) =>
  page.getByRole('row').filter({ has: page.getByText(name, { exact: true }) });
async function axe(page: Page) {
  await page.emulateMedia({ reducedMotion: 'reduce' });
  // Disable caret loops and the global 0.01ms transitions for a settled axe snapshot.
  await page.addStyleTag({
    content: '*, *::before, *::after { animation: none !important; transition: none !important; }',
  });
  await page.evaluate(async () => {
    await Promise.allSettled(document.getAnimations().map((animation) => animation.finished));
  });
  expect(
    (await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa', 'wcag21aa']).analyze())
      .violations,
  ).toEqual([]);
}
async function nativeUser(page: Page, name = 'test-user') {
  await page.getByRole('button', { name: 'Add user', exact: true }).click();
  const sheet = page.getByRole('dialog', { name: 'Create new user' });
  await sheet.getByLabel('Username', { exact: true }).fill(name);
  await sheet.getByRole('checkbox', { name: 'analysts', exact: true }).check();
  await sheet.locator('[data-dialog-confirm]').click();
  await expect(sheet.getByLabel('One-time password')).toHaveValue(/^demo-password-/);
  await expect(
    sheet.getByText('This is the only time you can see this password.', { exact: true }),
  ).toBeVisible();
  await sheet.getByRole('button', { name: 'Done', exact: true }).click();
  return name;
}
async function live(
  page: Page,
  options: { denied?: boolean; inspectionFails?: boolean; legacy?: boolean; noOidc?: boolean } = {},
) {
  await page.route('**/api/**', (route) => {
    const path = new URL(route.request().url()).pathname;
    if (path === '/api/v1/about')
      return route.fulfill({
        json: options.noOidc
          ? {}
          : { oidcActive: true, capabilities: { oidcRoleMapping: true, oidcRoleSync: true } },
      });
    if (path === '/api/v1/logstream') return route.fulfill({ json: [{ name: 'web_logs' }] });
    if (path === '/api/v1/role/default') return route.fulfill({ json: null });
    if (path === '/api/v1/roles')
      return route.fulfill({
        json: { readers: [{ privilege: 'reader', resource: { stream: 'web_logs' } }] },
      });
    if (path === '/api/v1/users')
      return route.fulfill(
        options.denied
          ? { status: 403, contentType: 'text/plain', body: 'Permission denied' }
          : {
              json: [
                {
                  id: 'issuer/sub',
                  username: 'oauth.user',
                  method: 'oauth',
                  email: null,
                  picture: null,
                  roles: { readers: [] },
                  groupRoles: {},
                  userGroups: [],
                },
              ],
            },
      );
    if (path === '/api/v1/user/issuer%2Fsub/role')
      return route.fulfill(
        options.inspectionFails
          ? { status: 503, contentType: 'text/plain', body: 'Cannot load role sources' }
          : {
              json: {
                roles: { readers: { actions: [], roleType: 'user' } },
                groupRoles: {},
                oidc: {
                  issuer: options.legacy ? null : 'https://issuer.example',
                  legacy: options.legacy === true,
                  groups: options.legacy ? null : ['readers'],
                  manualRoles: options.legacy ? null : [],
                  providerRoles: options.legacy ? null : ['readers'],
                  defaultRole: null,
                },
              },
            },
      );
    return route.fulfill({ status: 404, body: 'Not found' });
  });
}

test('Team is reachable through the Admin navigation', async ({ page }) => {
  await page.addInitScript(() => sessionStorage.setItem('parseable-mode', 'demo'));
  await page.goto('/datasets');
  await page.getByTestId('sidebar-team').click();
  await expect(page).toHaveURL(/\/team$/);
  await expect(page.getByRole('heading', { name: 'Access management' })).toBeVisible();
  await expect(page.getByRole('tab', { name: 'Roles', exact: true })).toHaveAttribute(
    'aria-selected',
    'true',
  );
});

test('roles support scoped creation, privilege addition and removal, empty roles and typed deletion', async ({
  page,
}) => {
  await demo(page);
  await page.getByRole('button', { name: 'Add role', exact: true }).click();
  const create = page.getByRole('dialog', { name: 'Create new role' });
  await create.getByLabel('Role name').fill('web-readers');
  await create.getByLabel('Privilege', { exact: true }).selectOption('reader');
  await create.getByLabel('Dataset', { exact: true }).selectOption('application_logs');
  await create.locator('[data-dialog-confirm]').click();
  const row = namedRow(page, 'web-readers');
  await expect(row).toContainText('reader of application_logs');
  await row.getByRole('button', { name: 'Add privilege to role web-readers' }).click();
  const add = page.getByRole('dialog', { name: 'Add privilege to role' });
  await add.getByLabel('Privilege', { exact: true }).selectOption('writer');
  await add.getByLabel('Dataset', { exact: true }).selectOption('*');
  await add.locator('[data-dialog-confirm]').click();
  await expect(row).toContainText('writer of all datasets');
  await expect(
    row.getByRole('button', { name: 'Add privilege to role web-readers' }),
  ).toBeFocused();
  for (const label of ['writer of all datasets', 'reader of application_logs']) {
    await row.getByRole('button', { name: `Remove ${label} from web-readers` }).click();
    await page
      .getByRole('dialog', { name: 'Remove privilege' })
      .locator('[data-dialog-confirm]')
      .click();
    await expect(row.getByText(label, { exact: true })).toHaveCount(0);
  }
  await expect(row).toContainText('No privileges');
  await row.getByRole('button', { name: 'Delete role web-readers' }).click();
  const remove = page.getByRole('dialog', { name: 'Delete role', exact: true });
  await expect(remove.locator('[data-dialog-confirm]')).toBeDisabled();
  await remove.getByLabel('Confirmation name').fill('Web-readers');
  await expect(remove.locator('[data-dialog-confirm]')).toBeDisabled();
  await remove.getByLabel('Confirmation name').fill('web-readers');
  await remove.locator('[data-dialog-confirm]').click();
  await expect(row).toHaveCount(0);
});

test('role creation rejects duplicate, reserved and shadowed names and supports multiple rows', async ({
  page,
}) => {
  await demo(page);
  await page.getByRole('button', { name: 'Add role', exact: true }).click();
  const sheet = page.getByRole('dialog', { name: 'Create new role' });
  await sheet.getByLabel('Role name').fill('analysts');
  await expect(sheet.getByRole('alert')).toHaveText('Role already exists');
  await sheet.getByLabel('Role name').fill('default');
  await expect(sheet.getByRole('alert')).toContainText('default OIDC role endpoint');
  await sheet.getByLabel('Role name').fill('ADMIN');
  await expect(sheet.getByRole('alert')).toHaveText('This name is reserved.');
  await sheet.getByLabel('Role name').fill('multi-role');
  await sheet.getByRole('button', { name: 'Add another privilege' }).click();
  await sheet
    .getByRole('group', { name: 'Privilege 2' })
    .getByLabel('Privilege', { exact: true })
    .selectOption('editor');
  await expect(
    sheet.getByRole('group', { name: 'Privilege 2' }).getByLabel('Dataset', { exact: true }),
  ).toHaveCount(0);
  await sheet.locator('[data-dialog-confirm]').click();
  await expect(namedRow(page, 'multi-role')).toContainText('editor');
  await expect(namedRow(page, 'multi-role')).toContainText('reader of all datasets');
});

test('users show a one-time password and support assignment, removal, reset and deletion', async ({
  page,
  context,
}) => {
  await context.grantPermissions(['clipboard-read', 'clipboard-write']);
  await demo(page, '/team?tab=user');
  const name = await nativeUser(page);
  const row = namedRow(page, name);
  await row.getByRole('button', { name: `Assign roles to ${name}` }).click();
  const assign = page.getByRole('dialog', { name: 'Assign roles', exact: true });
  await expect(assign.getByRole('checkbox', { name: 'analysts', exact: true })).toHaveCount(0);
  await assign.getByRole('checkbox', { name: 'editors', exact: true }).check();
  await assign.locator('[data-dialog-confirm]').click();
  await expect(row.getByText('editors', { exact: true })).toBeVisible();
  await expect(row.getByRole('button', { name: `Assign roles to ${name}` })).toBeFocused();
  await row.getByRole('button', { name: `Remove analysts from ${name}` }).click();
  await page
    .getByRole('dialog', { name: 'Remove role', exact: true })
    .getByRole('button', { name: 'Remove', exact: true })
    .click();
  await expect(row.getByText('analysts', { exact: true })).toHaveCount(0);
  await row.getByRole('button', { name: `Reset password for ${name}` }).click();
  const reset = page.getByRole('dialog', { name: 'Reset password', exact: true });
  await reset.getByLabel('Confirmation name').fill(name);
  await reset.locator('[data-dialog-confirm]').click();
  const password = await reset.getByLabel('One-time password').inputValue();
  expect(password).toMatch(/^demo-password-/);
  await reset.getByRole('button', { name: 'Copy password', exact: true }).click();
  await expect(reset.getByRole('status')).toHaveText('Password copied to clipboard');
  expect(await page.evaluate(() => navigator.clipboard.readText())).toBe(password);
  expect(
    await page.evaluate(() => JSON.stringify({ ...localStorage, ...sessionStorage })),
  ).not.toContain(password);
  await reset.getByRole('button', { name: 'Done', exact: true }).click();
  await row.getByRole('button', { name: `Delete user ${name}` }).click();
  const remove = page.getByRole('dialog', { name: 'Delete user', exact: true });
  await expect(remove.locator('[data-dialog-confirm]')).toBeDisabled();
  await remove.getByLabel('Confirmation name').fill(name);
  await remove.locator('[data-dialog-confirm]').click();
  await expect(row).toHaveCount(0);
});

for (const [mode, name] of [
  ['demo', 'admin'],
  ['live', 'frontend-smoke'],
]) {
  test(`${mode} root admin shows its badge and no user mutation actions`, async ({ page }) => {
    if (mode === 'demo') await demo(page, '/team?tab=user');
    else {
      await live(page);
      await page.route('**/api/v1/users', (route) =>
        route.fulfill({
          json: [
            {
              id: name,
              username: name,
              method: 'native',
              roles: { 'super-admin': [{ privilege: 'superadmin' }] },
              group_roles: {},
              user_groups: [],
            },
          ],
        }),
      );
      await page.goto('/team?tab=user');
    }
    const row = namedRow(page, name);
    const badge = row.getByText('Root admin', { exact: true });
    await expect(badge).toBeVisible();
    await expect(badge).toHaveAttribute('data-tone', 'neutral');
    await expect(row.getByText('super-admin', { exact: true })).toBeVisible();
    await expect(row.getByRole('button', { name: /^Remove / })).toHaveCount(0);
    await expect(row.getByRole('button', { name: `Assign roles to ${name}` })).toHaveCount(0);
    await expect(row.getByRole('button', { name: `Reset password for ${name}` })).toHaveCount(0);
    await expect(row.getByRole('button', { name: `Delete user ${name}` })).toHaveCount(0);
    await expect(row.getByText('Managed by server configuration', { exact: true })).toBeVisible();
  });
}

test('inherited roles are labelled and cannot be removed from the user row', async ({ page }) => {
  await demo(page, '/team?tab=user');
  const row = namedRow(page, 'analyst');
  await expect(row.getByText('Inherited from "analytics" group', { exact: true })).toBeVisible();
  await expect(row.getByRole('button', { name: 'Remove auditors from analyst' })).toHaveCount(0);
  await page.getByLabel('Search users').fill('ANALYST@EXAMPLE');
  await expect(page.getByRole('table').locator('tbody tr')).toHaveCount(1);
  await page.getByLabel('Search users').fill('auditors');
  await expect(page.getByRole('table').locator('tbody tr')).toHaveCount(2);
  await page.getByLabel('Search users').fill('');
  await page.getByRole('button', { name: 'Sort by method' }).click();
  await expect(page.getByRole('columnheader', { name: 'Sort by method' })).toHaveAttribute(
    'aria-sort',
    'ascending',
  );
  await page.getByRole('button', { name: 'Sort by email' }).click();
  await expect(page.getByRole('columnheader', { name: 'Sort by email' })).toHaveAttribute(
    'aria-sort',
    'ascending',
  );
});

test('OIDC provider-only roles stay disabled; manual roles can be removed; sources are viewable', async ({
  page,
}) => {
  await demo(page, '/team?tab=user');
  const row = namedRow(page, 'sso.user');
  await expect(row.getByRole('button', { name: /Reset password/ })).toHaveCount(0);
  await row.getByRole('button', { name: 'Remove provider-readers from sso.user' }).click();
  const dialog = page.getByRole('dialog', { name: 'Remove role', exact: true });
  await expect(
    dialog.getByText(
      'This role has no manual grant to remove. Update the provider group or default role configuration.',
      { exact: true },
    ),
  ).toBeVisible();
  await expect(dialog.getByRole('button', { name: 'Remove', exact: true })).toBeDisabled();
  await axe(page);
  await page.keyboard.press('Escape');
  await row.getByRole('button', { name: 'Remove auditors from sso.user' }).click();
  await expect(
    dialog.getByText('This removes the administrator’s manual grant.', { exact: true }),
  ).toBeVisible();
  await dialog.getByRole('button', { name: 'Remove', exact: true }).click();
  await expect(row.getByText('auditors', { exact: true })).toHaveCount(0);
  await row.getByRole('button', { name: 'View role sources for sso.user' }).click();
  const sheet = page.getByRole('dialog', { name: 'View role sources' });
  await expect(sheet.getByText('Provider groups: provider-readers', { exact: true })).toBeVisible();
  await expect(
    sheet.getByText('Roles assigned by administrators: None', { exact: true }),
  ).toBeVisible();
  await expect(
    sheet.getByText('Fallback role (when no manual or provider role): observers', { exact: true }),
  ).toBeVisible();
  await axe(page);
});

test('OIDC fallback roles can be assigned manually', async ({ page }) => {
  await demo(page, '/team?tab=user');
  const row = namedRow(page, 'sso.fallback');
  await expect(row.getByText('observers', { exact: true })).toBeVisible();
  await row.getByRole('button', { name: 'Assign roles to sso.fallback' }).click();
  const dialog = page.getByRole('dialog', { name: 'Assign roles', exact: true });
  await expect(dialog.getByRole('checkbox', { name: 'observers', exact: true })).toBeVisible();
});

test('OIDC inspection fails closed and a legacy account explains reauthentication', async ({
  page,
}) => {
  await live(page, { inspectionFails: true });
  await page.goto('/team?tab=user');
  await page.getByRole('button', { name: 'Remove readers from oauth.user' }).click();
  const dialog = page.getByRole('dialog', { name: 'Remove role', exact: true });
  await expect(dialog.getByRole('alert')).toHaveText('Cannot load role sources');
  await expect(dialog.getByRole('button', { name: 'Remove', exact: true })).toBeDisabled();
  await page.keyboard.press('Escape');
  await page.unroute('**/api/**');
  await live(page, { legacy: true });
  await page.reload();
  await page.getByRole('button', { name: 'Remove readers from oauth.user' }).click();
  await expect(
    dialog.getByText(
      'Role sources are not yet available. This user must sign in again to record current provider groups.',
      { exact: true },
    ),
  ).toBeVisible();
  await expect(dialog.getByRole('button', { name: 'Remove', exact: true })).toBeEnabled();
});

test('default OIDC role changes update the badge and can be cleared', async ({ page }) => {
  await demo(page);
  const opener = page.getByRole('button', { name: 'Manage default OIDC role' });
  await opener.click();
  const dialog = page.getByRole('dialog', { name: 'Default OIDC role', exact: true });
  await expect(dialog.getByText('Current default: observers', { exact: true })).toBeVisible();
  await dialog.getByLabel('Default role', { exact: true }).selectOption('analysts');
  await dialog.getByRole('button', { name: 'Set default', exact: true }).click();
  await expect(namedRow(page, 'analysts').getByText('Default', { exact: true })).toBeVisible();
  await opener.click();
  await expect(dialog.getByText('Current default: analysts', { exact: true })).toBeVisible();
  await dialog.getByRole('button', { name: 'Clear default', exact: true }).click();
  await expect(page.getByRole('table').getByText('Default', { exact: true })).toHaveCount(0);
  await opener.click();
  await expect(dialog.getByText('Current default: None', { exact: true })).toBeVisible();
  await expect(dialog.getByRole('button', { name: 'Clear default', exact: true })).toBeDisabled();
});

test('server failures stay inside typed confirmation dialogs', async ({ page }) => {
  await demo(page);
  await page.getByRole('button', { name: 'Delete role observers', exact: true }).click();
  const dialog = page.getByRole('dialog', { name: 'Delete role', exact: true });
  await dialog.getByLabel('Confirmation name').fill('observers');
  await dialog.locator('[data-dialog-confirm]').click();
  await expect(dialog.getByRole('alert')).toHaveText(
    'Clear or change the default OIDC role before deleting this role.',
  );
});

test('OIDC groups map exact role names and have a search empty state', async ({ page }) => {
  await demo(page, '/team?tab=groups');
  await expect(
    page.getByRole('heading', { name: 'Provider groups → Parseable roles' }),
  ).toBeVisible();
  await expect(
    page.getByText(/Provider and default grants refresh on sign-in and within five minutes/),
  ).toBeVisible();
  const row = namedRow(page, 'provider-readers');
  await expect(row.getByRole('cell')).toHaveText(['provider-readers', 'provider-readers']);
  await page.getByLabel('Search OIDC groups').fill('missing');
  await expect(page.getByText('No roles match your search.', { exact: true })).toBeVisible();
  await expect(page.getByRole('button', { name: /Add (group|role)/ })).toHaveCount(0);
});

test('API keys are shown once at creation, masked in the table and deleted by name', async ({
  page,
  context,
}) => {
  await context.grantPermissions(['clipboard-read', 'clipboard-write']);
  await demo(page, '/team?tab=apikeys');
  await expect(page.getByRole('table')).toContainText('****1234');
  await page.getByRole('button', { name: 'Add API key', exact: true }).click();
  const sheet = page.getByRole('dialog', { name: 'Add API key', exact: true });
  await sheet.getByLabel('API key name', { exact: true }).fill('automation-key');
  await sheet.getByRole('checkbox', { name: 'analysts', exact: true }).check();
  await sheet.locator('[data-dialog-confirm]').click();
  const key = await sheet.getByLabel('API key', { exact: true }).inputValue();
  expect(key).toBeTruthy();
  await expect(
    sheet.getByText(
      'This is the only time you can see this key. Store it securely; if it is lost, delete it and create another.',
    ),
  ).toBeVisible();
  await sheet.getByRole('button', { name: 'Copy API key', exact: true }).click();
  await expect(sheet.getByRole('status')).toHaveText('API key copied to clipboard');
  expect(await page.evaluate(() => navigator.clipboard.readText())).toBe(key);
  expect(
    await page.evaluate(() => JSON.stringify({ ...localStorage, ...sessionStorage })),
  ).not.toContain(key);
  await sheet.getByRole('button', { name: 'Done', exact: true }).click();
  const row = namedRow(page, 'automation-key');
  await expect(row).toContainText(`****${key.slice(-4)}`);
  await expect(page.getByText(key, { exact: true })).toHaveCount(0);
  // The key cannot be read back: the list has no copy action
  await expect(row.getByRole('button', { name: /^Copy API key/ })).toHaveCount(0);
  await row.getByRole('button', { name: 'Delete API key automation-key' }).click();
  const remove = page.getByRole('dialog', { name: 'Delete API key', exact: true });
  await expect(remove.locator('[data-dialog-confirm]')).toBeDisabled();
  await remove.getByLabel('Confirmation name').fill('automation-key');
  await remove.locator('[data-dialog-confirm]').click();
  await expect(row).toHaveCount(0);
});

for (const [tab, label] of [
  ['roles', 'Roles'],
  ['user', 'Users'],
  ['groups', 'OIDC groups'],
  ['apikeys', 'API keys'],
]) {
  test(`deep link and accessibility: ${tab}`, async ({ page }) => {
    await demo(page, `/team?tab=${tab}`);
    await expect(page.getByRole('tab', { name: label, exact: true })).toHaveAttribute(
      'aria-selected',
      'true',
    );
    await axe(page);
    await page.getByRole('button', { name: 'Use dark theme', exact: true }).click();
    await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark');
    await axe(page);
  });
}
for (const [tab, action, title] of [
  ['roles', 'Add role', 'Create new role'],
  ['user', 'Add user', 'Create new user'],
  ['apikeys', 'Add API key', 'Add API key'],
]) {
  test(`open dialog accessibility and Escape focus restoration: ${tab}`, async ({ page }) => {
    await demo(page, `/team?tab=${tab}`);
    const opener = page.getByRole('button', { name: action, exact: true });
    await opener.focus();
    await page.keyboard.press('Enter');
    await expect(page.getByRole('dialog', { name: title, exact: true })).toBeVisible();
    if (tab === 'roles')
      await expect(page.getByRole('dialog').getByLabel('Dataset', { exact: true })).toBeVisible();
    else await expect(page.getByRole('dialog').getByRole('checkbox').first()).toBeVisible();
    await axe(page);
    await page.keyboard.press('Escape');
    await expect(page.getByRole('dialog')).toHaveCount(0);
    await expect(opener).toBeFocused();
  });
}

test('invalid tabs replace with roles and preserve other search parameters', async ({ page }) => {
  await demo(page, '/team?tab=unknown&keep=value');
  await expect(page).toHaveURL(/\/team\?tab=roles&keep=value$/);
  await expect(page.getByRole('tab', { name: 'Roles', exact: true })).toHaveAttribute(
    'aria-selected',
    'true',
  );
});

test('unavailable OIDC groups fall back safely', async ({ page }) => {
  await live(page, { noOidc: true });
  await page.goto('/team?tab=groups');
  await expect(page).toHaveURL(/\/team\?tab=roles$/);
  await expect(page.getByRole('tab', { name: 'OIDC groups', exact: true })).toHaveCount(0);
  await expect(page.getByRole('button', { name: 'Manage default OIDC role' })).toHaveCount(0);
});

test('live 403 shows Permission denied and keeps the current page and mode', async ({ page }) => {
  await live(page, { denied: true });
  await page.goto('/team?tab=user');
  await expect(page.getByRole('heading', { name: 'Permission denied', exact: true })).toBeVisible();
  await expect(page).toHaveURL(/\/team\?tab=user$/);
  await expect(page.getByText('Live server', { exact: true })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Explore demo data' })).toHaveCount(0);
  await expect(page.getByRole('table')).toHaveCount(0);
});

test('Team pagination has 25 rows and search resets the current page', async ({ page }) => {
  await live(page, { noOidc: true });
  await page.route('**/api/v1/roles', (route) =>
    route.fulfill({
      json: Object.fromEntries(
        Array.from({ length: 32 }, (_, index) => [
          `role-${String(index + 1).padStart(3, '0')}`,
          [],
        ]),
      ),
    }),
  );
  await page.goto('/team');
  await expect(page.getByRole('table').locator('tbody tr')).toHaveCount(25);
  await expect(page.getByText('Showing 1–25 of 32 roles', { exact: true })).toBeVisible();
  await page.getByRole('button', { name: 'Next page', exact: true }).click();
  await expect(page.getByRole('table').locator('tbody tr')).toHaveCount(7);
  await expect(page.getByText('Showing 26–32 of 32 roles', { exact: true })).toBeVisible();
  await page.getByLabel('Search roles').fill('role-003');
  await expect(page.getByText('Showing 1–1 of 1 roles', { exact: true })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Previous page', exact: true })).toBeDisabled();
});

for (const [tab, noun, endpoint, emptyCopy, searchCopy] of [
  [
    'roles',
    'roles',
    '/api/v1/roles',
    'Add a role to define access.',
    'No roles found matching your search.',
  ],
  [
    'user',
    'users',
    '/api/v1/users',
    'No users yet. Add a user to grant access.',
    'No users found matching your search.',
  ],
  [
    'groups',
    'OIDC groups',
    '/api/v1/roles',
    'Create a role in the Roles tab to map a provider group.',
    'No roles match your search.',
  ],
  [
    'apikeys',
    'API keys',
    '/api/prism/v1/apikeys',
    'No API keys yet. Add an API key to grant programmatic access.',
    'No API keys found matching your search.',
  ],
]) {
  test(`empty ${noun} explain how to add items and hide pagination`, async ({ page }) => {
    await live(page);
    await page.route(`**${endpoint}`, (route) =>
      route.fulfill({ json: tab === 'roles' || tab === 'groups' ? {} : [] }),
    );
    await page.goto(`/team?tab=${tab}`);
    await expect(page.getByLabel(`Search ${noun}`)).toHaveValue('');
    await expect(page.getByText(emptyCopy, { exact: true })).toBeVisible();
    await expect(page.getByText(searchCopy, { exact: true })).toHaveCount(0);
    await expect(page.getByText(/^Showing /)).toHaveCount(0);
    await expect(page.locator('.table-footer')).toHaveCount(0);
  });

  test(`non-matching ${noun} search explains the search miss and hides pagination`, async ({
    page,
  }) => {
    await demo(page, `/team?tab=${tab}`);
    const search = page.getByLabel(`Search ${noun}`);
    await expect(page.getByText(/^Showing /)).toBeVisible();
    await search.fill('missing-team-item');
    await expect(page.getByText(searchCopy, { exact: true })).toBeVisible();
    await expect(page.getByText(emptyCopy, { exact: true })).toHaveCount(0);
    await expect(page.getByText(/^Showing /)).toHaveCount(0);
    await expect(page.locator('.table-footer')).toHaveCount(0);
    await search.fill('');
    await expect(page.getByText(searchCopy, { exact: true })).toHaveCount(0);
    await expect(page.getByText(/^Showing /)).toBeVisible();
  });
}

test('an invalid tab is replaced even if the capability request fails', async ({ page }) => {
  await live(page);
  await page.route('**/api/v1/about', (route) =>
    route.fulfill({ status: 403, body: 'Permission denied' }),
  );
  await page.goto('/team?tab=invalid');
  await expect(page).toHaveURL(/\/team\?tab=roles$/);
  await expect(page.getByRole('heading', { name: 'Permission denied' })).toBeVisible();
});

test('Escape cancels typed deletion and returns focus to row actions', async ({ page }) => {
  await demo(page);
  const opener = page.getByRole('button', { name: 'Delete role writers', exact: true });
  await opener.focus();
  await page.keyboard.press('Enter');
  const dialog = page.getByRole('dialog', { name: 'Delete role', exact: true });
  await dialog.getByLabel('Confirmation name').fill('writers');
  await page.keyboard.press('Escape');
  await expect(dialog).toHaveCount(0);
  await expect(namedRow(page, 'writers')).toBeVisible();
  await expect(opener).toBeFocused();
});

async function expectPendingDialog(page: Page, dialog: Locator) {
  await page.keyboard.press('Escape');
  await expect(dialog).toBeVisible();
  const close = dialog.getByRole('button', { name: 'Close dialog', exact: true });
  await expect(close).toBeDisabled();
  await close.click({ force: true });
  await expect(dialog).toBeVisible();
  await page.locator('.ui-dialog-overlay').click({ position: { x: 5, y: 5 } });
  await expect(dialog).toBeVisible();
}

for (const kind of ['user', 'apikey']) {
  test(`pending ${kind} creation keeps its secret visible until dismissed`, async ({ page }) => {
    await live(page);
    const native = kind === 'user';
    const endpoint = native ? '/api/v1/user/new-user' : '/api/prism/v1/apikeys';
    const secret = 'late-test-secret';
    let release = () => {};
    const held = new Promise<void>((done) => {
      release = done;
    });
    await page.route(`**${endpoint}`, async (route) => {
      if (route.request().method() !== 'POST') return route.fulfill({ json: [] });
      await held;
      await route.fulfill(
        native
          ? { contentType: 'text/plain', body: secret }
          : {
              json: {
                keyId: 'new-key',
                keyName: 'new-user',
                apiKey: secret,
                roles: ['readers'],
                createdBy: 'admin',
                createdAt: '2026-10-09T00:00:00Z',
                modifiedAt: '2026-10-09T00:00:00Z',
              },
            },
      );
    });
    await page.goto(`/team?tab=${native ? 'user' : 'apikeys'}`);
    const opener = page.getByRole('button', {
      name: native ? 'Add user' : 'Add API key',
      exact: true,
    });
    await opener.click();
    const sheet = page.getByRole('dialog', {
      name: native ? 'Create new user' : 'Add API key',
      exact: true,
    });
    const name = sheet.getByLabel(native ? 'Username' : 'API key name', { exact: true });
    const result = sheet.getByLabel(native ? 'One-time password' : 'API key', { exact: true });
    await name.fill('new-user');
    await sheet.getByRole('checkbox', { name: 'readers', exact: true }).check();
    const request = page.waitForRequest(
      (entry) => entry.method() === 'POST' && entry.url().endsWith(endpoint),
    );
    await sheet.locator('[data-dialog-confirm]').click();
    await request;
    await expectPendingDialog(page, sheet);
    release();
    await expect(result).toHaveValue(secret);
    await sheet.getByRole('button', { name: 'Done', exact: true }).click();
    await expect(sheet).toHaveCount(0);
    await opener.click();
    await expect(name).toHaveValue('');
    await expect(result).toHaveCount(0);
  });
}

for (const fails of [false, true]) {
  test(`pending password reset allows dismissal only after ${fails ? 'failure' : 'showing the password'}`, async ({
    page,
  }) => {
    await live(page);
    await page.route('**/api/v1/users', (route) =>
      route.fulfill({
        json: [
          {
            id: 'reset-user',
            username: 'reset-user',
            method: 'native',
            roles: { readers: [] },
            groupRoles: {},
            userGroups: [],
          },
        ],
      }),
    );
    let release = () => {};
    const held = new Promise<void>((done) => {
      release = done;
    });
    const endpoint = '/api/v1/user/reset-user/generate-new-password';
    await page.route(`**${endpoint}`, async (route) => {
      await held;
      await route.fulfill({
        status: fails ? 503 : 200,
        contentType: 'text/plain',
        body: fails ? 'Reset failed' : 'new-test-password',
      });
    });
    await page.goto('/team?tab=user');
    const opener = page.getByRole('button', { name: 'Reset password for reset-user', exact: true });
    await opener.click();
    const dialog = page.getByRole('dialog', { name: 'Reset password', exact: true });
    await dialog.getByLabel('Confirmation name').fill('reset-user');
    const request = page.waitForRequest((entry) => entry.url().endsWith(endpoint));
    await dialog.locator('[data-dialog-confirm]').click();
    await request;
    await expect(dialog.getByRole('button', { name: 'Cancel', exact: true })).toBeDisabled();
    await expectPendingDialog(page, dialog);
    release();
    if (fails) await expect(dialog.getByRole('alert')).toHaveText('Reset failed');
    else await expect(dialog.getByLabel('One-time password')).toHaveValue('new-test-password');
    await expect(dialog.getByRole('button', { name: 'Close dialog', exact: true })).toBeEnabled();
    await page.keyboard.press('Escape');
    await expect(dialog).toHaveCount(0);
    await opener.click();
    await expect(dialog.getByLabel('Confirmation name')).toHaveValue('');
    await expect(dialog.getByLabel('One-time password')).toHaveCount(0);
  });
}
