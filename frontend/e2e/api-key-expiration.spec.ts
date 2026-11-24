import { test, expect, type Page } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

const keysPath = '/api/prism/v1/apikeys';
const now = '2026-10-12T12:00:00.000Z';
function key(name: string, expiresAt?: string | null) {
  return {
    keyId: name,
    keyName: name,
    apiKey: '****1234',
    roles: ['readers'],
    createdBy: 'admin',
    createdAt: now,
    modifiedAt: now,
    ...(expiresAt === undefined ? {} : { expiresAt }),
  };
}
const row = (page: Page, name: string) =>
  page.getByRole('row').filter({ has: page.getByText(name, { exact: true }) });
async function live(
  page: Page,
  initial: ReturnType<typeof key>[] = [],
  legacy: false | 'omit' | 'null' = false,
) {
  let keys = [...initial];
  const posts: unknown[] = [];
  const deletes: string[] = [];
  await page.route('**/api/**', async (route) => {
    const request = route.request();
    const path = new URL(request.url()).pathname;
    if (path === keysPath) {
      if (request.method() === 'POST') {
        const body = request.postDataJSON();
        posts.push(body);
        const expiresAt =
          body.ttlSeconds === null
            ? null
            : new Date(Date.parse(now) + body.ttlSeconds * 1000).toISOString();
        const result = key(
          body.keyName,
          legacy === 'omit' ? undefined : legacy === 'null' ? null : expiresAt,
        );
        keys.push(result);
        return route.fulfill({ json: { ...result, apiKey: 'secret-key-1234' } });
      }
      return route.fulfill({ json: keys });
    }
    if (path.startsWith(`${keysPath}/`) && request.method() === 'DELETE') {
      const id = decodeURIComponent(path.slice(keysPath.length + 1));
      deletes.push(id);
      keys = keys.filter((entry) => entry.keyId !== id);
      return route.fulfill({ status: 200, body: '' });
    }
    if (path === '/api/v1/about') return route.fulfill({ json: {} });
    if (path === '/api/v1/logstream') return route.fulfill({ json: [] });
    if (path === '/api/v1/role/default') return route.fulfill({ json: null });
    if (path === '/api/v1/roles') return route.fulfill({ json: { readers: [] } });
    if (path === '/api/v1/users') return route.fulfill({ json: [] });
    return route.fulfill({ status: 404, body: 'Not found' });
  });
  await page.goto('/team?tab=apikeys');
  await expect(page.getByRole('table')).toBeVisible();
  return { posts, deletes };
}
async function create(page: Page, name: string) {
  await page.getByRole('button', { name: 'Add API key', exact: true }).click();
  const sheet = page.getByRole('dialog', { name: 'Add API key', exact: true });
  await sheet.getByLabel('API key name', { exact: true }).fill(name);
  await sheet.getByRole('checkbox', { name: 'readers', exact: true }).check();
  return sheet;
}

for (const selection of [
  { name: 'default 90 days', seconds: 7776000 },
  { name: 'minimum 30 minutes', preset: '1800', seconds: 1800 },
  { name: 'custom 45 minutes', duration: '45', unit: 'minutes', seconds: 2700 },
  { name: 'custom 2 hours', duration: '2', unit: 'hours', seconds: 7200 },
  { name: 'custom 1.1 hours', duration: '1.1', unit: 'hours', seconds: 3960 },
  { name: 'no expiration', preset: 'none', seconds: null },
]) {
  test(`API key creation sends ${selection.name} and displays server expiration`, async ({
    page,
  }) => {
    const { posts } = await live(page);
    const sheet = await create(page, 'automation');
    await expect(sheet.getByLabel('Expiration', { exact: true })).toHaveValue('7776000');
    if (selection.preset)
      await sheet.getByLabel('Expiration', { exact: true }).selectOption(selection.preset);
    if (selection.duration) {
      await sheet.getByLabel('Expiration', { exact: true }).selectOption('custom');
      await sheet.getByLabel('Duration unit', { exact: true }).selectOption(selection.unit!);
      await sheet.getByLabel('Duration', { exact: true }).fill(selection.duration);
    }
    await sheet.getByRole('button', { name: 'Create API key', exact: true }).click();
    await expect(sheet.getByLabel('API key', { exact: true })).toHaveValue('secret-key-1234');
    expect(posts).toEqual([
      { keyName: 'automation', roles: ['readers'], ttlSeconds: selection.seconds },
    ]);
    const expiresAt =
      selection.seconds === null
        ? null
        : new Date(Date.parse(now) + selection.seconds * 1000).toISOString();
    if (expiresAt) await expect(sheet.locator('time')).toHaveAttribute('datetime', expiresAt);
    else await expect(sheet.getByText('No expiration.', { exact: true })).toBeVisible();
    await sheet.getByRole('button', { name: 'Done', exact: true }).click();
    if (expiresAt)
      await expect(row(page, 'automation').locator(`time[datetime="${expiresAt}"]`)).toBeVisible();
    else await expect(row(page, 'automation')).toContainText('No expiration');
  });
}

test('custom duration rejects out-of-range and fractional-second values without POSTing', async ({
  page,
}) => {
  const { posts } = await live(page);
  const sheet = await create(page, 'invalid-duration');
  await sheet.getByLabel('Expiration', { exact: true }).selectOption('custom');
  for (const [value, unit] of [
    ['29', 'minutes'],
    ['91', 'days'],
    ['30.001', 'minutes'],
    ['', 'hours'],
  ]) {
    await sheet.getByLabel('Duration unit', { exact: true }).selectOption(unit);
    await sheet.getByLabel('Duration', { exact: true }).fill(value);
    await expect(sheet.getByRole('alert')).toHaveText(
      'Enter a duration from 30 minutes to 90 days, in whole seconds.',
    );
    await expect(sheet.getByRole('button', { name: 'Create API key', exact: true })).toBeDisabled();
  }
  expect(posts).toEqual([]);
  await sheet.getByLabel('Duration', { exact: true }).fill('0.5');
  await expect(sheet.getByRole('button', { name: 'Create API key', exact: true })).toBeEnabled();
});

test('custom duration input range follows the selected unit', async ({ page }) => {
  await live(page);
  const sheet = await create(page, 'bounded-duration');
  await sheet.getByLabel('Expiration', { exact: true }).selectOption('custom');
  const duration = sheet.getByLabel('Duration', { exact: true });
  for (const [unit, min, max] of [
    ['minutes', '30', '129600'],
    ['hours', '0.5', '2160'],
    ['days', String(1 / 48), '90'],
  ]) {
    await sheet.getByLabel('Duration unit', { exact: true }).selectOption(unit);
    await expect(duration).toHaveAttribute('min', min);
    await expect(duration).toHaveAttribute('max', max);
    await expect(duration).toHaveAttribute('step', 'any');
    await duration.fill(min);
    expect(await duration.evaluate((input: HTMLInputElement) => input.validity.valid)).toBe(true);
    await expect(sheet.getByRole('button', { name: 'Create API key', exact: true })).toBeEnabled();
  }
  await sheet.getByLabel('Duration unit', { exact: true }).selectOption('minutes');
  await duration.fill('29');
  expect(await duration.evaluate((input: HTMLInputElement) => input.validity.rangeUnderflow)).toBe(
    true,
  );
  await duration.fill('129601');
  expect(await duration.evaluate((input: HTMLInputElement) => input.validity.rangeOverflow)).toBe(
    true,
  );
});

test('expiration updates at the deadline, disables copying and preserves deletion and legacy keys', async ({
  page,
}) => {
  await page.clock.install({ time: new Date(now) });
  const expiration = '2026-10-12T12:00:30.000Z';
  const { deletes } = await live(page, [
    key('soon', expiration),
    key('legacy'),
    key('forever', null),
  ]);
  const soon = row(page, 'soon');
  await expect(soon.getByText('Expired', { exact: true })).toHaveCount(0);
  await page.clock.fastForward(30_000);
  await expect(soon.getByText('Expired', { exact: true })).toBeVisible();
  for (const name of ['legacy', 'forever']) {
    await expect(row(page, name)).toContainText('No expiration');
  }
  // The key is shown once, when it is created: no row offers to copy it
  await expect(page.getByRole('button', { name: /^Copy API key/ })).toHaveCount(0);
  await soon.getByRole('button', { name: 'Delete API key soon' }).click();
  const dialog = page.getByRole('dialog', { name: 'Delete API key', exact: true });
  await dialog.getByLabel('Confirmation name').fill('soon');
  await dialog.locator('[data-dialog-confirm]').click();
  await expect(soon).toHaveCount(0);
  expect(deletes).toEqual(['soon']);
});

for (const legacy of ['omit', 'null'] as const) {
  test(`finite key creation warns when server expiration is ${legacy}`, async ({ page }) => {
    const { posts } = await live(page, [], legacy);
    const sheet = await create(page, 'legacy-create');
    await sheet.getByRole('button', { name: 'Create API key', exact: true }).click();
    await expect(sheet.getByLabel('API key', { exact: true })).toHaveValue('secret-key-1234');
    expect(posts).toEqual([{ keyName: 'legacy-create', roles: ['readers'], ttlSeconds: 7776000 }]);
    await expect(
      sheet.getByText(
        'The server did not apply expiration. This key does not expire; delete it if that is not intended.',
        { exact: true },
      ),
    ).toBeVisible();
    await expect(sheet.locator('time')).toHaveCount(0);
    await sheet.getByRole('button', { name: 'Done', exact: true }).click();
    await expect(row(page, 'legacy-create')).toContainText('No expiration');
  });
}

test('expiration controls are accessible on mobile and absent for native users', async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await live(page);
  const sheet = await create(page, 'mobile-key');
  await sheet.getByLabel('Expiration', { exact: true }).selectOption('custom');
  await sheet.getByLabel('Duration unit', { exact: true }).selectOption('minutes');
  await sheet.getByLabel('Duration', { exact: true }).fill('45');
  await page.emulateMedia({ reducedMotion: 'reduce' });
  expect(
    (await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa', 'wcag21aa']).analyze())
      .violations,
  ).toEqual([]);
  await page.keyboard.press('Escape');
  await page.getByRole('tab', { name: 'Users', exact: true }).click();
  await page.getByRole('button', { name: 'Add user', exact: true }).click();
  const user = page.getByRole('dialog', { name: 'Create new user', exact: true });
  await expect(user.getByLabel('Username', { exact: true })).toBeVisible();
  await expect(user.getByLabel('Expiration', { exact: true })).toHaveCount(0);
  await expect(user.getByLabel('Duration', { exact: true })).toHaveCount(0);
});
