package org.embulk.filter;

import java.text.Normalizer;
import java.util.Set;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
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
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.util.LineEncoder;

public class NormalizeFilterPlugin
        implements FilterPlugin
{
    public interface PluginTask
            extends Task, LineEncoder.EncoderTask, TimestampFormatter.FormatterTask
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
    public void transaction(
            ConfigSource config,
            Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        // TODO: inputSchema must include columns

        Schema outputSchema = inputSchema;
        control.run(task.dump(), outputSchema);
    }

    @Override
    public PageOutput open(
            TaskSource taskSource,
            final Schema inputSchema,
            final Schema outputSchema,
            final PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        return new NormalizePageOutput(task, inputSchema, outputSchema, output);
    }

    private class NormalizePageOutput
            implements PageOutput
    {
        private final Schema inputSchema;
        private final Schema outputSchema;
        private final PageOutput output;

        private final PageReader reader;
        private final Set<String> normalizeColumns;
        private final Normalizer.Form form;
        private final boolean trim;

        public NormalizePageOutput(
                PluginTask task,
                Schema inputSchema,
                Schema outputSchema,
                PageOutput output)
        {
            this.inputSchema = inputSchema;
            this.outputSchema = outputSchema;
            this.output = output;

            reader = new PageReader(inputSchema);
            normalizeColumns = task.getColumns();
            form = task.getForm();
            trim = task.getTrim();
        }

        @Override
        public void add(Page page)
        {
            try (final PageBuilder builder =
                         new PageBuilder(Exec.getBufferAllocator(), outputSchema, output)) {
                ColumnVisitor visitor = new NormalizeColumnVisitor(builder);
                reader.setPage(page);
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

        private class NormalizeColumnVisitor
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
    }
}
