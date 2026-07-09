import { useQuery } from "@tanstack/react-query";
import { ChevronDown } from "lucide-react";
import { useEffect } from "react";

import type { RoutineFieldInfo } from "@/lib/api";
import { api } from "@/lib/api";
import { useServer } from "@/hooks/useServer";

interface Props {
  fields: Record<string, RoutineFieldInfo>;
  values: Record<string, unknown>;
  onChange: (key: string, value: unknown) => void;
}

function SelectField({
  field,
  value,
  onChange,
}: {
  field: RoutineFieldInfo;
  value: unknown;
  onChange: (v: unknown) => void;
}) {
  const { server } = useServer();
  const { data, isLoading } = useQuery({
    queryKey: ["routine-field-options", field.options_from, server],
    queryFn: () => api.getRoutineFieldOptions(field.options_from!, server!),
    enabled: !!field.options_from && !!server,
    staleTime: 30_000,
  });

  const options = data?.options ?? [];

  // Auto-select first option when options load and no value is set
  useEffect(() => {
    if (options.length > 0 && (!value || value === "")) {
      onChange(options[0]);
    }
  }, [options, value, onChange]);

  return (
    <div className="relative">
      <select
        value={String(value ?? field.default ?? "")}
        onChange={(e) => onChange(e.target.value)}
        className="w-full appearance-none rounded border border-[var(--color-border)] bg-[var(--color-surface)] px-3 py-1.5 pr-8 text-sm text-[var(--color-text)] focus:border-[var(--color-primary)] focus:outline-none"
      >
        {isLoading && <option value="">Loading...</option>}
        {!isLoading && options.length === 0 && (
          <option value="">No options available</option>
        )}
        {options.map((opt) => (
          <option key={opt} value={opt}>
            {opt}
          </option>
        ))}
      </select>
      <ChevronDown className="pointer-events-none absolute right-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-[var(--color-text-muted)]" />
    </div>
  );
}

function MultiselectField({
  field,
  value,
  onChange,
}: {
  field: RoutineFieldInfo;
  value: unknown;
  onChange: (v: unknown) => void;
}) {
  const { server } = useServer();
  const { data, isLoading } = useQuery({
    queryKey: ["routine-field-options", field.options_from, server],
    queryFn: () => api.getRoutineFieldOptions(field.options_from!, server!),
    enabled: !!field.options_from && !!server,
    staleTime: 30_000,
  });

  const options = data?.options ?? [];
  // Value is stored as a CSV string (matches the routine's Pydantic `str`
  // field). Empty = "all selected"; any non-empty = filter to those items.
  const raw = String(value ?? field.default ?? "");
  const selected = new Set(
    raw.split(",").map((s) => s.trim()).filter(Boolean),
  );
  const emit = (next: Set<string>) => onChange(Array.from(next).sort().join(","));

  return (
    <div className="max-h-48 overflow-y-auto rounded border border-[var(--color-border)] bg-[var(--color-surface)] p-2">
      {isLoading && (
        <div className="text-xs text-[var(--color-text-muted)]">Loading...</div>
      )}
      {!isLoading && options.length === 0 && (
        <div className="text-xs text-[var(--color-text-muted)]">
          No options available
        </div>
      )}
      {!isLoading && options.length > 0 && (
        <div className="mb-1 flex gap-2 border-b border-[var(--color-border)] pb-1 text-xs">
          <button
            type="button"
            onClick={() => emit(new Set(options))}
            className="text-[var(--color-primary)] hover:underline"
          >
            All
          </button>
          <button
            type="button"
            onClick={() => emit(new Set())}
            className="text-[var(--color-text-muted)] hover:underline"
          >
            None
          </button>
          <span className="ml-auto text-[var(--color-text-muted)]">
            {selected.size === 0 ? "all" : `${selected.size}/${options.length}`}
          </span>
        </div>
      )}
      {options.map((opt) => {
        const checked = selected.has(opt);
        return (
          <label
            key={opt}
            className="flex cursor-pointer items-center gap-2 py-0.5 text-sm text-[var(--color-text)]"
          >
            <input
              type="checkbox"
              checked={checked}
              onChange={() => {
                const next = new Set(selected);
                if (checked) next.delete(opt);
                else next.add(opt);
                emit(next);
              }}
            />
            <span>{opt}</span>
          </label>
        );
      })}
    </div>
  );
}

export function RoutineConfigForm({ fields, values, onChange }: Props) {
  return (
    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
      {Object.entries(fields).map(([key, field]) => (
        <div key={key}>
          <label className="mb-1 block text-xs font-medium text-[var(--color-text-muted)]">
            {field.description || key}
          </label>
          {field.widget === "select" && field.options_from ? (
            <SelectField
              field={field}
              value={values[key]}
              onChange={(v) => onChange(key, v)}
            />
          ) : field.widget === "multiselect" && field.options_from ? (
            <MultiselectField
              field={field}
              value={values[key]}
              onChange={(v) => onChange(key, v)}
            />
          ) : field.type === "bool" ? (
            <button
              type="button"
              onClick={() => onChange(key, !values[key])}
              className={`rounded px-3 py-1.5 text-xs font-medium transition-colors ${
                values[key]
                  ? "bg-[var(--color-primary)]/20 text-[var(--color-primary)]"
                  : "bg-[var(--color-surface-hover)] text-[var(--color-text-muted)]"
              }`}
            >
              {values[key] ? "ON" : "OFF"}
            </button>
          ) : field.type === "int" || field.type === "float" ? (
            <input
              type="number"
              step={field.type === "float" ? "any" : "1"}
              value={String(values[key] ?? field.default ?? "")}
              onChange={(e) => {
                const v = field.type === "int" ? parseInt(e.target.value) : parseFloat(e.target.value);
                if (!isNaN(v)) onChange(key, v);
              }}
              className="w-full rounded border border-[var(--color-border)] bg-[var(--color-surface)] px-3 py-1.5 text-sm text-[var(--color-text)] focus:border-[var(--color-primary)] focus:outline-none"
            />
          ) : (
            <input
              type="text"
              value={String(values[key] ?? field.default ?? "")}
              onChange={(e) => onChange(key, e.target.value)}
              className="w-full rounded border border-[var(--color-border)] bg-[var(--color-surface)] px-3 py-1.5 text-sm text-[var(--color-text)] focus:border-[var(--color-primary)] focus:outline-none"
            />
          )}
        </div>
      ))}
    </div>
  );
}
