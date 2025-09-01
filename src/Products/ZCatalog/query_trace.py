import os
import time
import gc
import logging
import traceback
import pprint

TRACE_CATALOG_QUERIES = bool(os.environ.get('TRACE_CATALOG_QUERIES'))

# Set up logging
logger = logging.getLogger('Products.ZCatalog.query_trace')
if TRACE_CATALOG_QUERIES:
    handler = logging.Handler()
    handler.setFormatter(logging.Formatter(
        '%(asctime)s %(levelname)s %(message)s\n'
        '    Query parameters:\n%(query_params)s\n'
        '    Query from:\n%(caller_stack)s\n'
        '    Details:\n%(query_details)s'
    ))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def get_caller_info(skip=3):
    """Get formatted call stack, skipping framework levels."""
    stack = []
    for frame in traceback.extract_stack()[:-skip]:
        # Skip internal Zope/ZCatalog frames
        if '/Products/ZCatalog/' in frame.filename:
            continue
        if '/Zope/' in frame.filename:
            continue
        
        stack.append(f'    File "{frame.filename}", line {frame.lineno}, in {frame.name}')
        if frame.line:
            stack.append(f'      {frame.line}')
    return '\n'.join(stack)


class QueryStepResult:
    """A result from one step of a catalog query."""

    def __init__(self, step_name, elapsed_time=None, result_size=None,
                persistent_load=None, gc_objects=None):
        self.step_name = step_name
        self.elapsed_time = elapsed_time
        self.result_size = result_size
        self.persistent_load = persistent_load
        self.gc_objects = gc_objects

    def __str__(self):
        parts = [
            f'Step: {self.step_name}',
            f'Time: {self.elapsed_time:.4f}s'
        ]
        if self.result_size is not None:
            parts.append(f'Results: {self.result_size}')
        if self.persistent_load is not None:
            parts.append(f'DB Objects: {self.persistent_load:+d}')
        if self.gc_objects is not None:
            parts.append(f'GC Objects: {self.gc_objects:+d}')
        return ' | '.join(parts)


class QueryTracer:
    """Traces execution of catalog queries."""

    def __init__(self, catalog, query=None):
        self._conn = catalog._p_jar
        self._results = []
        self._start_time = time.time()
        self._query = pprint.pformat(query, indent=6)
        # Track initial state
        self._last_db_objects = self._count_db_objects()
        self._last_gc_objects = len(gc.get_objects())

    def _count_db_objects(self):
        """Count objects in ZODB connection cache."""
        return len(self._conn._cache)

    def add_step(self, step_name, result=None):
        """Record metrics for a query step."""
        now = time.time()
        elapsed = now - self._start_time
        db_objects = self._count_db_objects()
        gc_objects = len(gc.get_objects())
        
        r = QueryStepResult(
            step_name,
            elapsed_time=elapsed,
            result_size=len(result) if result is not None else None,
            persistent_load=db_objects - self._last_db_objects,
            gc_objects=gc_objects - self._last_gc_objects
        )
        self._results.append(r)
        
        # Update state for next measurement
        self._last_db_objects = db_objects
        self._last_gc_objects = gc_objects
        return r

    def log_results(self):
        """Get all recorded steps."""
        total_time = self._results[-1].elapsed_time - self._results[0].elapsed_time
        steps_text = '\n      ' + '\n      '.join(str(step) for step in self._results)
        logger.info(
            'Catalog query completed in %.4fs',
            total_time,
            extra={
                'query_params': self._query,
                'caller_stack': get_caller_info(),
                'query_details': steps_text
            }
        )
        return self._results


def get_tracer(catalog, query):
    """Get a query tracer if query tracing is enabled."""
    if TRACE_CATALOG_QUERIES:
        return QueryTracer(catalog, query)
    return None
