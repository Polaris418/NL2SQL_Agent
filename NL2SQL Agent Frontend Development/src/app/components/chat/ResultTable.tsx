import { useMemo, useState } from 'react';
import { ArrowDown, ArrowUp, ArrowUpDown, ChevronLeft, ChevronRight, Download } from 'lucide-react';
import { Button } from '../ui/button';
import { Card } from '../ui/card';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../ui/select';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../ui/table';
import type { ExecutionResult } from '../../types/query';

interface ResultTableProps {
  result: ExecutionResult;
  onPageChange?: (pageNumber: number, pageSize: number) => void;
}

type SortDirection = 'asc' | 'desc' | null;

function getTotalRows(result: ExecutionResult) {
  return result.pagination?.total_count ?? result.pagination?.total_row_count ?? result.total_row_count ?? result.row_count;
}

function getExecutionTime(result: ExecutionResult) {
  return result.execution_time_ms ?? result.db_latency_ms ?? 0;
}

function escapeCsvCell(value: unknown) {
  const text = String(value ?? '');
  return text.includes(',') || text.includes('"') || text.includes('\n') ? `"${text.replace(/"/g, '""')}"` : text;
}

function formatExecutionTimeLabel(result: ExecutionResult) {
  const value = getExecutionTime(result);
  if (!value) {
    return '0ms';
  }
  if (value >= 1000) {
    return `${(value / 1000).toFixed(value >= 10000 ? 1 : 2)}s`;
  }
  if (value >= 100) {
    return `${Math.round(value)}ms`;
  }
  return `${value.toFixed(2)}ms`;
}

function getCellText(cell: unknown) {
  if (cell === null || cell === undefined) {
    return 'null';
  }
  if (typeof cell === 'object') {
    return JSON.stringify(cell);
  }
  return String(cell);
}

function getColumnWidthClass(column: string, rows: Record<string, unknown>[]) {
  const lowerName = column.toLowerCase();
  if (/(id|count|num|age|price|amount|score|level|status)$/.test(lowerName)) {
    return 'min-w-[96px]';
  }
  const sample = rows.find((row) => row?.[column] !== null && row?.[column] !== undefined)?.[column];
  const sampleLength = getCellText(sample).length;
  if (sampleLength > 40 || /(password|email|token|url|path|avatar|image|remark|description|content)/.test(lowerName)) {
    return 'min-w-[220px]';
  }
  if (sampleLength > 18) {
    return 'min-w-[160px]';
  }
  return 'min-w-[120px]';
}

export function ResultTable({ result, onPageChange }: ResultTableProps) {
  const [sortColumn, setSortColumn] = useState<string | null>(null);
  const [sortDirection, setSortDirection] = useState<SortDirection>(null);
  const [localPageSize, setLocalPageSize] = useState(5);
  const [localPage, setLocalPage] = useState(0);

  const useBackendPagination = Boolean(result.pagination);
  const canRemotePage = useBackendPagination && typeof onPageChange === 'function';
  const currentPage = useBackendPagination ? (result.pagination?.page_number || 1) - 1 : localPage;
  const pageSize = useBackendPagination ? (result.pagination?.page_size || 5) : localPageSize;
  const totalRows = getTotalRows(result);
  const totalPages = Math.max(1, Math.ceil(totalRows / Math.max(pageSize, 1)));

  const displayRows = useMemo(() => {
    let rows = [...result.rows];

    if (!useBackendPagination && sortColumn && sortDirection) {
      rows.sort((a, b) => {
        const aVal = a?.[sortColumn];
        const bVal = b?.[sortColumn];

        if (aVal === null || aVal === undefined) return sortDirection === 'asc' ? 1 : -1;
        if (bVal === null || bVal === undefined) return sortDirection === 'asc' ? -1 : 1;

        if (typeof aVal === 'number' && typeof bVal === 'number') {
          return sortDirection === 'asc' ? aVal - bVal : bVal - aVal;
        }

        const aStr = String(aVal).toLowerCase();
        const bStr = String(bVal).toLowerCase();
        return sortDirection === 'asc' ? aStr.localeCompare(bStr) : bStr.localeCompare(aStr);
      });
    }

    if (!useBackendPagination) {
      const start = localPage * localPageSize;
      rows = rows.slice(start, start + localPageSize);
    }

    return rows;
  }, [result.rows, sortColumn, sortDirection, localPage, localPageSize, useBackendPagination]);

  const handleSort = (column: string) => {
    if (sortColumn === column) {
      setSortDirection((prev) => {
        if (prev === 'asc') return 'desc';
        if (prev === 'desc') return null;
        return 'asc';
      });
      if (sortDirection === 'desc') {
        setSortColumn(null);
      }
      return;
    }
    setSortColumn(column);
    setSortDirection('asc');
  };

  const handlePageChange = (newPage: number) => {
    if (canRemotePage) {
      onPageChange?.(newPage + 1, pageSize);
      return;
    }
    setLocalPage(newPage);
  };

  const handlePageSizeChange = (newSize: string) => {
    const size = Number.parseInt(newSize, 10);
    if (canRemotePage) {
      onPageChange?.(1, size);
      return;
    }
    setLocalPageSize(size);
    setLocalPage(0);
  };

  const handleExportCSV = () => {
    const csvContent = [result.columns.join(','), ...result.rows.map((row) => result.columns.map((column) => escapeCsvCell(row?.[column])).join(','))].join('\n');
    const blob = new Blob(['\ufeff' + csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `query_result_${Date.now()}.csv`;
    link.click();
    URL.revokeObjectURL(url);
  };

  const SortIcon = ({ column }: { column: string }) => {
    if (sortColumn !== column) {
      return <ArrowUpDown className="h-3 w-3 ml-1 opacity-50" />;
    }
    if (sortDirection === 'asc') {
      return <ArrowUp className="h-3 w-3 ml-1" />;
    }
    if (sortDirection === 'desc') {
      return <ArrowDown className="h-3 w-3 ml-1" />;
    }
    return <ArrowUpDown className="h-3 w-3 ml-1 opacity-50" />;
  };

  return (
    <Card className="mb-4 min-w-0 max-w-full overflow-hidden border-emerald-200 shadow-[0_10px_24px_rgba(16,185,129,0.08)]">
      <div className="flex items-center justify-between gap-2 border-b border-emerald-100 bg-emerald-50/80 p-4 flex-wrap">
        <div className="flex items-center gap-4">
          <div>
            <div className="text-[11px] uppercase tracking-[0.24em] text-emerald-700/70">Result Set</div>
            <span className="mt-1 block text-sm font-semibold text-emerald-950">
              查询结果 ({result.row_count.toLocaleString()} 行)
            </span>
          </div>
          <span className="rounded-full border border-emerald-200 bg-white px-2.5 py-1 text-xs text-emerald-800">
            执行耗时: {formatExecutionTimeLabel(result)}
          </span>
        </div>
        <div className="flex items-center gap-2 flex-wrap">
          <div className="flex items-center gap-2">
            <span className="text-xs text-muted-foreground">每页：</span>
            <Select value={String(pageSize)} onValueChange={handlePageSizeChange}>
              <SelectTrigger className="h-8 w-20 border-zinc-300 bg-white text-sm font-medium text-black shadow-none">
                <SelectValue className="text-black" />
              </SelectTrigger>
              <SelectContent className="border-zinc-200 bg-white text-black">
                <SelectItem value="5" className="text-black">5</SelectItem>
                <SelectItem value="10" className="text-black">10</SelectItem>
                <SelectItem value="50" className="text-black">50</SelectItem>
                <SelectItem value="100" className="text-black">100</SelectItem>
                <SelectItem value="200" className="text-black">200</SelectItem>
              </SelectContent>
            </Select>
          </div>
          {totalPages > 1 && (!useBackendPagination || canRemotePage) && (
            <div className="flex items-center gap-2">
              <Button variant="outline" size="sm" onClick={() => handlePageChange(currentPage - 1)} disabled={currentPage === 0}>
                <ChevronLeft className="h-4 w-4" />
              </Button>
              <span className="text-sm whitespace-nowrap">
                {currentPage + 1} / {totalPages}
              </span>
              <Button variant="outline" size="sm" onClick={() => handlePageChange(currentPage + 1)} disabled={currentPage >= totalPages - 1}>
                <ChevronRight className="h-4 w-4" />
              </Button>
            </div>
          )}
          <Button variant="outline" size="sm" onClick={handleExportCSV}>
            <Download className="h-4 w-4 mr-1" />
            导出 CSV
          </Button>
        </div>
      </div>

      <div className="overflow-x-auto max-h-[600px] overflow-y-auto bg-white">
        <Table className="min-w-max table-auto">
          <TableHeader className="sticky top-0 z-10 bg-emerald-50">
            <TableRow>
              {result.columns.map((column) => (
                <TableHead
                  key={column}
                  className={`${getColumnWidthClass(column, result.rows)} whitespace-nowrap text-emerald-950 ${useBackendPagination ? '' : 'cursor-pointer hover:bg-emerald-100/60'}`}
                  onClick={() => !useBackendPagination && handleSort(column)}
                >
                  <div className="flex items-center">
                    {column}
                    {!useBackendPagination && <SortIcon column={column} />}
                  </div>
                </TableHead>
              ))}
            </TableRow>
          </TableHeader>
          <TableBody>
            {displayRows.length === 0 ? (
              <TableRow>
                <TableCell colSpan={Math.max(result.columns.length, 1)} className="py-8 text-center text-muted-foreground">
                  暂无数据
                </TableCell>
              </TableRow>
            ) : (
              displayRows.map((row, rowIndex) => (
                <TableRow key={rowIndex}>
                  {result.columns.map((column) => {
                    const cell = row?.[column];
                    const text = getCellText(cell);
                    return (
                      <TableCell key={column} className={`${getColumnWidthClass(column, result.rows)} align-top`}>
                        {cell === null || cell === undefined ? (
                          <span className="text-muted-foreground italic">null</span>
                        ) : typeof cell === 'object' ? (
                          <div className="max-w-[320px] overflow-hidden text-ellipsis whitespace-nowrap text-xs font-mono" title={text}>
                            {text}
                          </div>
                        ) : (
                          <div
                            className={`overflow-hidden text-ellipsis whitespace-nowrap ${
                              text.length > 48 ? 'max-w-[320px]' : 'max-w-[220px]'
                            }`}
                            title={text}
                          >
                            {text}
                          </div>
                        )}
                      </TableCell>
                    );
                  })}
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </div>
    </Card>
  );
}
