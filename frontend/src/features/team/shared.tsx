import { useEffect, useRef, useState } from 'react';
import { Copy, Search } from 'lucide-react';
import { Button, Input, PAGE_SIZE } from '../../components/ui';

export function useTeamSearch<T>(items: T[], matches: (item: T, query: string) => boolean) {
  const [search, setSearch] = useState('');
  const [requestedPage, setPage] = useState(0);
  const filtered = items.filter((item) => matches(item, search.toLowerCase()));
  const page = Math.min(requestedPage, Math.max(0, Math.ceil(filtered.length / PAGE_SIZE) - 1));
  return {
    search,
    setSearch: (value: string) => {
      setSearch(value);
      setPage(0);
    },
    page,
    setPage,
    total: filtered.length,
    rows: filtered.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE),
  };
}

export function TeamSearch({
  noun,
  placeholder,
  value,
  onChange,
}: {
  noun: string;
  placeholder: string;
  value: string;
  onChange: (value: string) => void;
}) {
  return (
    <div className="list-toolbar">
      <div className="search-field">
        <Search size={16} aria-hidden="true" />
        <Input
          aria-label={`Search ${noun}`}
          placeholder={placeholder}
          value={value}
          onChange={(event) => onChange(event.target.value)}
        />
      </div>
    </div>
  );
}

export function RoleCheckboxes({
  roles,
  selected,
  onChange,
  disabled,
}: {
  roles: string[];
  selected: string[];
  onChange: (value: string[]) => void;
  disabled?: boolean;
}) {
  return (
    <fieldset className="team-role-picker" disabled={disabled}>
      <legend>Roles</legend>
      <div className="team-checkbox-list">
        {roles.map((name) => (
          <label key={name}>
            <input
              type="checkbox"
              checked={selected.includes(name)}
              onChange={(event) =>
                onChange(
                  event.target.checked
                    ? [...selected, name]
                    : selected.filter((role) => role !== name),
                )
              }
            />
            {name}
          </label>
        ))}
        {!roles.length && <p className="muted">No roles available.</p>}
      </div>
    </fieldset>
  );
}

export function SecretResult({ value, kind }: { value: string; kind: 'password' | 'API key' }) {
  const [status, setStatus] = useState('');
  const active = useRef(true);
  useEffect(() => {
    active.current = true;
    return () => {
      active.current = false;
    };
  }, []);
  async function copy() {
    try {
      await navigator.clipboard.writeText(value);
      if (active.current)
        setStatus(`${kind === 'password' ? 'Password' : 'API key'} copied to clipboard`);
    } catch {
      if (active.current) setStatus(`Failed to copy ${kind}`);
    }
  }
  return (
    <div className="stack">
      <Input label={kind === 'password' ? 'One-time password' : 'API key'} value={value} readOnly />
      <Button onClick={() => void copy()} aria-label={`Copy ${kind}`}>
        <Copy size={15} aria-hidden="true" /> Copy {kind}
      </Button>
      <p className="notice">
        {kind === 'password'
          ? 'This is the only time you can see this password.'
          : 'This is the only time you can see this key. Store it securely; if it is lost, delete it and create another.'}
      </p>
      <p role="status">{status}</p>
    </div>
  );
}
