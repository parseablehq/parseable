import { useCallback, useEffect, useState } from 'react';
import {
  Badge,
  Button,
  Card,
  EmptyState,
  Pagination,
  TypedConfirmDialog,
} from '../../components/ui';
import { QueryState } from '../../components/explorer/QueryState';
import { useApp } from '../../app/AppProvider';
import type { ApiKey } from '../../lib/types';
import { CredentialSheet } from './CredentialSheet';
import { TeamSearch, useTeamSearch } from './shared';
import { useCollection } from '../../hooks/useCollection';
import { useMutation } from '../../hooks/useMutation';
import { isApiKeyExpired } from './apiKeyExpiration';

function DeleteKey({
  apiKey,
  onClose,
  onChanged,
}: {
  apiKey: ApiKey;
  onClose: () => void;
  onChanged: () => void;
}) {
  const { client } = useApp();
  const mutation = useMutation();
  function confirm() {
    void mutation.run(async () => {
      await client.deleteApiKey(apiKey.keyId);
      if (mutation.isActive()) onClose();
      onChanged();
    });
  }
  return (
    <TypedConfirmDialog
      open
      title="Delete API key"
      name={apiKey.keyName}
      action="Delete API key"
      onOpenChange={(open) => {
        if (!open) onClose();
      }}
      onConfirm={confirm}
      pending={mutation.pending}
      error={mutation.error}
    />
  );
}

export function ApiKeysTab({
  creating,
  closeCreate,
}: {
  creating: boolean;
  closeCreate: () => void;
}) {
  const { client } = useApp();
  const keys = useCollection(useCallback((signal) => client.listApiKeys(signal), [client]));
  const [deleting, setDeleting] = useState<ApiKey>();
  const [now, setNow] = useState(Date.now);
  useEffect(() => {
    const current = Date.now();
    const upcoming = (keys.data ?? [])
      .map((key) => Date.parse(key.expiresAt ?? ''))
      .filter((time) => time > current);
    if (!upcoming.length) return;
    const timer = window.setTimeout(
      () => setNow(Date.now()),
      Math.min(Math.min(...upcoming) - current, 2_147_483_647),
    );
    return () => window.clearTimeout(timer);
  }, [keys.data, now]);
  const list = useTeamSearch(
    [...(keys.data ?? [])].sort((a, b) => a.keyName.localeCompare(b.keyName)),
    (key, search) => key.keyName.toLowerCase().includes(search),
  );
  return (
    <section aria-label="API keys" className="team-tab">
      <TeamSearch
        noun="API keys"
        placeholder="Search by name"
        value={list.search}
        onChange={list.setSearch}
      />
      <QueryState loading={keys.loading && !keys.data} error={keys.error} retry={keys.reload} />
      {keys.loading && keys.data && (
        <p role="status" className="muted">
          Refreshing API keys…
        </p>
      )}
      {keys.data && (
        <Card className="team-table" aria-busy={keys.loading}>
          <div className="table-scroll">
            <table>
              <caption className="sr-only">API keys</caption>
              <thead>
                <tr>
                  <th scope="col">Name</th>
                  <th scope="col">Roles</th>
                  <th scope="col">Created by</th>
                  <th scope="col">Created at</th>
                  <th scope="col">Expiration</th>
                  <th scope="col">Actions</th>
                </tr>
              </thead>
              <tbody>
                {list.rows.map((key) => (
                  <tr key={key.keyId}>
                    <td>
                      <div className="team-key-name">
                        <strong>{key.keyName}</strong>
                        <code>{key.apiKey}</code>
                      </div>
                    </td>
                    <td>
                      <div className="team-chips">
                        {key.roles.map((role) => (
                          <Badge key={role}>{role}</Badge>
                        ))}
                      </div>
                    </td>
                    <td>{key.createdBy}</td>
                    <td>
                      <time dateTime={key.createdAt}>
                        {new Date(key.createdAt).toLocaleString()}
                      </time>
                    </td>
                    <td>
                      {key.expiresAt ? (
                        <div className="stack">
                          <time dateTime={key.expiresAt}>
                            {new Date(key.expiresAt).toLocaleString()}
                          </time>
                          {isApiKeyExpired(key.expiresAt, Math.max(now, Date.now())) && (
                            <Badge>Expired</Badge>
                          )}
                        </div>
                      ) : (
                        'No expiration'
                      )}
                    </td>
                    <td>
                      <div className="team-actions">
                        <Button
                          variant="ghost"
                          size="sm"
                          aria-label={`Delete API key ${key.keyName}`}
                          onClick={() => setDeleting(key)}
                        >
                          Delete API key
                        </Button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {!list.total && (
            <EmptyState
              title="No API keys found"
              description={
                keys.data.length
                  ? 'No API keys found matching your search.'
                  : 'No API keys yet. Add an API key to grant programmatic access.'
              }
            />
          )}
          <Pagination
            page={list.page}
            total={list.total}
            noun="API keys"
            onPageChange={list.setPage}
          />
        </Card>
      )}
      {creating && <CredentialSheet kind="apikey" onClose={closeCreate} onChanged={keys.reload} />}
      {deleting && (
        <DeleteKey
          apiKey={deleting}
          onClose={() => setDeleting(undefined)}
          onChanged={keys.reload}
        />
      )}
    </section>
  );
}
