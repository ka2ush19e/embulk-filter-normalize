package org.embulk.filter;

import java.text.Normalizer;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;

public class NormalizeFilterPlugin
        implements FilterPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("columns")
        Set<String> getColumns();

        @Config("form")
        @ConfigDefault("\"NFKC\"")
        Normalizer.Form getForm();

        @Config("trim")
        @ConfigDefault("true")
        boolean getTrim();
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema, FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for (Column inputColumn : inputSchema.getColumns()) {
            builder.add(inputColumn.getName());
        }
        Set<String> inputColumnNames = builder.build();
        for (String name : task.getColumns()) {
            if (!inputColumnNames.contains(name)) {
                throw new ConfigException("Column " + name + " is not included in input columns");
            }
        }

        control.run(task.dump(), inputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, final Schema inputSchema,
            final Schema outputSchema, final PageOutput output)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        final Set<String> normalizeColumns = task.getColumns();
        final Normalizer.Form form = task.getForm();
        final boolean trim = task.getTrim();

        return new PageOutput()
        {
            private final PageReader reader = new PageReader(inputSchema);

            @Override
            public void add(Page page)
            {
                reader.setPage(page);
                try (final PageBuilder builder = new PageBuilder(
                        Exec.getBufferAllocator(), outputSchema, output)) {
                    ColumnVisitor visitor = new NormalizeColumnVisitor(builder);
                    while (reader.nextRecord()) {
                        inputSchema.visitColumns(visitor);
                        builder.addRecord();
                    }
                    builder.flush(); // XXX: finish => NullPointerException on next page
                }
            }

            @Override
            public void finish()
            {
                output.finish();
            }

            @Override
            public void close()
            {
                reader.close();
                output.close();
            }

            class NormalizeColumnVisitor
                    implements ColumnVisitor
            {
                private final PageBuilder builder;

                public NormalizeColumnVisitor(PageBuilder builder)
                {
                    this.builder = builder;
                }

                @Override
                public void booleanColumn(Column column)
                {
                    if (reader.isNull(column)) {
                        builder.setNull(column);
                    } else {
                        builder.setBoolean(column, reader.getBoolean(column));
                    }
                }

                @Override
                public void longColumn(Column column)
                {
                    if (reader.isNull(column)) {
                        builder.setNull(column);
                    } else {
                        builder.setLong(column, reader.getLong(column));
                    }
                }

                @Override
                public void doubleColumn(Column column)
                {
                    if (reader.isNull(column)) {
                        builder.setNull(column);
                    } else {
                        builder.setDouble(column, reader.getDouble(column));
                    }
                }

                @Override
                public void stringColumn(Column column)
                {
                    if (reader.isNull(column)) {
                        builder.setNull(column);
                    } else {
                        if (normalizeColumns.contains(column.getName())) {
                            String normalized = Normalizer.normalize(reader.getString(column), form);
                            if (trim) {
                                normalized = normalized.trim();
                            }
                            if (normalized.isEmpty()) {
                                builder.setNull(column);
                            } else {
                                builder.setString(column, normalized);
                            }
                        } else {
                            builder.setString(column, reader.getString(column));
                        }
                    }
                }

                @Override
                public void timestampColumn(Column column)
                {
                    if (reader.isNull(column)) {
                        builder.setNull(column);
                    } else {
                        builder.setTimestamp(column, reader.getTimestamp(column));
                    }
                }
            }
        };
    }
}
