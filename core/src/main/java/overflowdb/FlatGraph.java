package overflowdb;

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import com.github.luben.zstd.ZstdDirectBufferDecompressingStream;
import scala.collection.immutable.ArraySeq;
import com.github.luben.zstd.Zstd;

public class FlatGraph {
    public static class NodeHandle {
        //give this a proper type later
        public final Object graph;
        public final short kindId;
        //offset into property arrays
        public final int seqId;
        //allow to mark that a NodeHandle is deleted
        public byte flag;

        public NodeHandle(Object graph, short kindId, int seqId) {
            this.graph = graph;
            this.kindId = kindId;
            this.seqId = seqId;
        }
    }

    public static enum Disposition {
        PROPERTY, EDGE_OUT, EDGE_IN
    }

    public static enum ContentType {
        BOOL, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, STRING, REF
    }

    public static enum Cardinality {
        //ZERO, //we currently don't need/support that, but long term we should allow the option
        ONE, MAYBE, MULTI
    }

    public static class Property {
        public final String nodeLabel;
        public final String propertyLabel;
        public final Disposition disposition;
        public Object qty;
        public Object values;
        public ContentType type = null;
        public Cardinality cardinality = null;

        public Property(String nodeLabel, String propertyLabel, Disposition disposition) {
            this.nodeLabel = nodeLabel;
            this.propertyLabel = propertyLabel;
            this.disposition = disposition;
        }


        public void initFromConversion(int[] quantities, ArrayList<?> values) {
            Map.Entry<Object, Object[]> qtyAdjusted = compressQty(quantities, values);
            this.qty = qtyAdjusted.getKey();
            if (qty == null) this.cardinality = Cardinality.ONE;
            else if (qty instanceof int[]) this.cardinality = Cardinality.MULTI;
            else if (qty instanceof boolean[]) this.cardinality = Cardinality.MAYBE;

            this.values = homogenize(qtyAdjusted.getValue());
            if (this.values instanceof boolean[]) this.type = ContentType.BOOL;
            else if (this.values instanceof byte[]) this.type = ContentType.BYTE;
            else if (this.values instanceof short[]) this.type = ContentType.SHORT;
            else if (this.values instanceof int[]) this.type = ContentType.INT;
            else if (this.values instanceof long[]) this.type = ContentType.LONG;
            else if (this.values instanceof float[]) this.type = ContentType.FLOAT;
            else if (this.values instanceof double[]) this.type = ContentType.DOUBLE;
            else if (this.values instanceof String[]) this.type = ContentType.STRING;
            else if (this.values instanceof NodeHandle[]) this.type = ContentType.REF;
        }

        static Map.Entry<Object, Object[]> compressQty(int[] qty, ArrayList<?> values) {
            boolean hasZero = false;
            boolean hasMulti = false;
            for (int i = 0; i < qty.length; i += 1) {
                hasZero |= qty[i] == 0;
                hasMulti |= qty[i] > 1;
            }
            if (hasMulti) {
                int counter = 0;
                for (int i = 0; i < qty.length; i += 1) {
                    counter += qty[i];
                    qty[i] = counter;
                    //FIXHERE
                }
                return new AbstractMap.SimpleEntry<>(qty, values.toArray());
            } else if (hasZero) {
                Object[] resVal = new Object[qty.length];
                boolean[] resQty = new boolean[qty.length];
                int offset = 0;
                for (int i = 0; i < qty.length; i += 1) {
                    if (qty[i] != 0) {
                        resQty[i] = true;
                        resVal[i] = values.get(offset);
                        offset += 1;
                    }
                }
                return new AbstractMap.SimpleEntry<>(qty, resVal);
            } else {
                return new AbstractMap.SimpleEntry<>(null, values.toArray());
            }
        }

        static Object homogenize(Object[] values) {
            for (Object value : values) {
                if (value == null) continue;
                else if (value instanceof Boolean) return homogenizeBool(values);
                else if (value instanceof Byte) return homogenizeByte(values);
                else if (value instanceof Short) return homogenizeShort(values);
                else if (value instanceof Integer) return homogenizeInt(values);
                else if (value instanceof Long) return homogenizeLong(values);
                else if (value instanceof Float) return homogenizeFloat(values);
                else if (value instanceof Double) return homogenizeDouble(values);
                else if (value instanceof String) {
                    String[] res = new String[values.length];
                    System.arraycopy(values, 0, res, 0, values.length);
                    return res;
                } else if (value instanceof NodeHandle) {
                    NodeHandle[] res = new NodeHandle[values.length];
                    System.arraycopy(values, 0, res, 0, values.length);
                    return res;
                }
            }
            return null;
        }

        static Object homogenizeBool(Object[] values) {
            boolean[] res = new boolean[values.length];
            for (int i = 0; i < values.length; i += 1) {
                if (values[i] != null) {
                    res[i] = (Boolean) values[i];
                }
            }
            return res;
        }

        static Object homogenizeByte(Object[] values) {
            byte[] res = new byte[values.length];
            for (int i = 0; i < values.length; i += 1) {
                if (values[i] != null) {
                    res[i] = (Byte) values[i];
                }
            }
            return res;
        }

        static Object homogenizeShort(Object[] values) {
            short[] res = new short[values.length];
            for (int i = 0; i < values.length; i += 1) {
                if (values[i] != null) {
                    res[i] = (Short) values[i];
                }
            }
            return res;
        }

        static Object homogenizeInt(Object[] values) {
            int[] res = new int[values.length];
            for (int i = 0; i < values.length; i += 1) {
                if (values[i] != null) {
                    res[i] = (Integer) values[i];
                }
            }
            return res;
        }

        static Object homogenizeLong(Object[] values) {
            long[] res = new long[values.length];
            for (int i = 0; i < values.length; i += 1) {
                if (values[i] != null) {
                    res[i] = (Long) values[i];
                }
            }
            return res;
        }

        static Object homogenizeFloat(Object[] values) {
            float[] res = new float[values.length];
            for (int i = 0; i < values.length; i += 1) {
                if (values[i] != null) {
                    res[i] = (Float) values[i];
                }
            }
            return res;
        }

        static Object homogenizeDouble(Object[] values) {
            double[] res = new double[values.length];
            for (int i = 0; i < values.length; i += 1) {
                if (values[i] != null) {
                    res[i] = (Double) values[i];
                }
            }
            return res;
        }
    }

    public static class FlatGraphManifest implements Serializable {
        public String[] nodeLabels;
        public int[] nodeCounts;
        public StorageProperty[] properties;
        public SerializedOffsets stringPoolLengths;
        public SerializedOffsets stringPoolData;


        public FlatGraphManifest(String[] nodeLabels, int[] nodeCounts, StorageProperty[] properties) {
            this.nodeLabels = nodeLabels;
            this.nodeCounts = nodeCounts;
            this.properties = properties;
            this.stringPoolLengths = new SerializedOffsets();
            this.stringPoolData = new SerializedOffsets();
        }

        final public static class StorageProperty implements Serializable {
            public final String nodeLabel;
            public final String propertyLabel;
            public final Disposition disposition;
            public final ContentType type;
            public final Cardinality cardinality;
            public final SerializedOffsets qtyOffsets;
            public final SerializedOffsets valuesOffsets;

            public StorageProperty(String nodeLabel, String propertyLabel, Disposition disposition, ContentType type, Cardinality cardinality) {
                this.nodeLabel = nodeLabel;
                this.propertyLabel = propertyLabel;
                this.disposition = disposition;
                this.type = type;
                this.cardinality = cardinality;
                this.qtyOffsets = new SerializedOffsets();
                this.valuesOffsets = new SerializedOffsets();
            }
        }
    }

    static public class StoreJob implements Runnable {
        Object inputdata;
        FileChannel dst;
        SerializedOffsets offsets;
        AtomicLong filePointer;
        boolean diffInt;

        public StoreJob(Object inputdata, FileChannel dst, SerializedOffsets offsets, AtomicLong filePointer, boolean diffInt) {
            this.inputdata = inputdata;
            this.dst = dst;
            this.offsets = offsets;
            this.filePointer = filePointer;
            this.diffInt = diffInt;
        }

        @Override
        public void run() {
            if (inputdata instanceof FlatGraphWIP.ReadJob) {
                inputdata = ((FlatGraphWIP.ReadJob) inputdata).runWithResult();
            }
            assert (offsets.start == 0L);
            assert (offsets.compressedLen == 0L);
            assert (offsets.decompressedLen == 0L);
            byte[] byteData;
            if (inputdata instanceof boolean[]) {
                boolean[] data = (boolean[]) inputdata;
                byteData = new byte[data.length];
                for (int i = 0; i < data.length; i++) {
                    byteData[i] = data[i] ? (byte) 1 : (byte) 0;
                }
            } else if (inputdata instanceof byte[]) {
                byteData = (byte[]) inputdata;
            } else if (inputdata instanceof short[]) {
                short[] data = (short[]) inputdata;
                byteData = new byte[2 * data.length];
                ByteBuffer.wrap(byteData).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer().put(data);
            } else if (inputdata instanceof int[]) {
                int[] data = (int[]) inputdata;
                byteData = new byte[4 * data.length];
                if (!diffInt)
                    ByteBuffer.wrap(byteData).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().put(data);
                else {
                    //FIXHERE
                    IntBuffer buf = ByteBuffer.wrap(byteData).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
                    if (data.length > 0)
                        buf.put(0, data[0]);
                    for (int i = 1; i < data.length; i += 1) {
                        buf.put(i, data[i] - data[i - 1]);
                    }

                }
            } else if (inputdata instanceof long[]) {
                long[] data = (long[]) inputdata;
                byteData = new byte[8 * data.length];
                ByteBuffer.wrap(byteData).order(ByteOrder.LITTLE_ENDIAN).asLongBuffer().put(data);
            } else if (inputdata instanceof float[]) {
                float[] data = (float[]) inputdata;
                byteData = new byte[4 * data.length];
                ByteBuffer.wrap(byteData).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().put(data);
            } else if (inputdata instanceof double[]) {
                double[] data = (double[]) inputdata;
                byteData = new byte[8 * data.length];
                ByteBuffer.wrap(byteData).order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer().put(data);
            } else if (inputdata instanceof NodeHandle[]) {
                NodeHandle[] data = (NodeHandle[]) inputdata;
                byteData = new byte[8 * data.length];
                LongBuffer buf = ByteBuffer.wrap(byteData).order(ByteOrder.LITTLE_ENDIAN).asLongBuffer();
                for (int i = 0; i < data.length; i += 1) {
                    NodeHandle handle = data[i];
                    if (handle == null) {
                        buf.put(i, -1L);
                    } else {
                        buf.put(i, (((long) handle.kindId) << 32) | ((long) handle.seqId));
                    }
                }
            } else {
                if (inputdata == null)
                    throw new NullPointerException();
                else
                    throw new RuntimeException("Unexpected data type for serialization: " + inputdata.getClass());
            }
            offsets.decompressedLen = byteData.length;
            byte[] compressed = Zstd.compress(byteData);
            //just making sure...
            byte[] dec = Zstd.decompress(compressed, byteData.length);
            offsets.compressedLen = compressed.length;
            offsets.start = filePointer.getAndAdd(offsets.compressedLen);
            ByteBuffer buf = ByteBuffer.wrap(compressed);
            long outPos = offsets.start;
            while (buf.hasRemaining()) {
                try {
                    outPos += dst.write(buf, outPos);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            assert (outPos == offsets.start + offsets.compressedLen);
            inputdata = null;
            dst = null;
        }
    }

    public static class MMapCollection {
        ArrayList<MappedByteBuffer> mmaps = new ArrayList<>();
        ArrayList<Long> mmapStarts = new ArrayList<>();
        static public int MAXSIZE = 3<<29; //1.5GB. round number, large enough for realistic cpgs, far away from integer overflow

        public MMapCollection(ArrayList<Long> offsets, FileChannel file) throws java.io.IOException {
            offsets.sort(Long::compareTo);
            long curBase = 0;
            long curOff = 0;
            for (int i = 0; i < offsets.size(); i += 1) {
                long off = offsets.get(i);
                if (off - curBase > MAXSIZE) {
                    mmaps.add(file.map(FileChannel.MapMode.READ_ONLY, curBase, curOff - curBase));
                    mmapStarts.add(curBase);
                    curBase = curOff;
                }
                curOff = off;
                if (curOff - curBase > MAXSIZE)
                    throw new RuntimeException("Cannot MMAP file segment: Max size " + MAXSIZE + " but requested " + (curOff - curBase));
            }
            if (curBase != curOff) {
                mmaps.add(file.map(FileChannel.MapMode.READ_ONLY, curBase, curOff - curBase));
                mmapStarts.add(curBase);
            }
            mmapStarts.add(curOff);

        }

        public ByteBuffer get(long offset, long len) {
            int i = mmapStarts.size()-1;
            while (mmapStarts.get(i) > offset) i -= 1;
            ByteBuffer buf = mmaps.get(i).duplicate();
            long base = mmapStarts.get(i);
            if (base + buf.limit() < offset + len) {
                StringBuilder b = new StringBuilder();
                b.append("Requested MMAP for off: ").append(offset).append(" len: ").append(len).append("\nAvailable mappings: ");
                for (int j = 0; j < mmaps.size(); j += 1) {
                    if (j == i - 1) b.append(" [");
                    b.append(" (from: ").append(mmapStarts.get(j)).append(" to: ").append(mmapStarts.get(j + 1))
                            .append(" intendedLen: ").append(mmapStarts.get(j + 1) - mmapStarts.get(j))
                            .append(" reportedLen: ").append(mmaps.get(j).limit()).append(" ) ");
                    if (j == i - 1) b.append("] ");
                }
                throw new RuntimeException(b.toString());
            }
            buf.position((int) (offset - base)).limit((int) (offset + len - base));
            return buf;
        }

        public ByteBuffer get(SerializedOffsets offsets) {
            return get(offsets.start, offsets.compressedLen);
        }

    }


    static public class FlatGraphWIP {
        public ArrayList<Property> properties;
        public ArrayList<String> nodeLabels;
        public NodeHandle[][] nodes;
        public FlatGraphManifest debugManifest;

        public FlatGraphManifest makeManifest() {
            String[] labels = nodeLabels.toArray(new String[0]);
            int[] counts = new int[nodes.length];
            for (int i = 0; i < counts.length; i += 1) {
                counts[i] = nodes[i].length;
            }
            FlatGraphManifest.StorageProperty[] props = new FlatGraphManifest.StorageProperty[properties.size()];
            for (int i = 0; i < props.length; i += 1) {
                Property p = properties.get(i);
                props[i] = new FlatGraphManifest.StorageProperty(p.nodeLabel, p.propertyLabel, p.disposition, p.type, p.cardinality);
            }
            return new FlatGraphManifest(labels, counts, props);
        }


        public FlatGraphWIP() {
        }

        public FlatGraphWIP(FileChannel src, boolean eager) {
            readGraph(src, eager);
        }

        public void readGraph(FileChannel src, boolean eager) {
            try {
                long manifestPos;
                FlatGraphManifest manifest;
                {
                    ByteBuffer header = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
                    int readBytes = 0;
                    while (readBytes < 16) {
                        readBytes += src.read(header, (long) readBytes);
                    }
                    header.clear();
                    if (header.getLong() != 0xdeadbeefdeadbeefL) throw new RuntimeException();
                    manifestPos = header.getLong();
                    int manifestSize = (int) (src.size() - manifestPos);
                    byte[] manifestBytes = new byte[manifestSize];
                    {
                        ByteBuffer manifestBuffer = ByteBuffer.wrap(manifestBytes);
                        readBytes = 0;
                        while (readBytes < manifestSize) {
                            readBytes += src.read(manifestBuffer, manifestPos + readBytes);
                        }
                    }
                    manifest = (FlatGraphManifest) (new ObjectInputStream(new ByteArrayInputStream(manifestBytes))).readObject();
                    this.debugManifest = manifest;
                }
                //allocate nodes
                this.nodes = new NodeHandle[manifest.nodeCounts.length][];
                for (int i = 0; i < manifest.nodeCounts.length; i += 1) {
                    int sz = manifest.nodeCounts[i];
                    NodeHandle[] nnodes = new NodeHandle[sz];
                    this.nodes[i] = nnodes;
                    for (int j = 0; j < sz; j += 1) {
                        nnodes[j] = new NodeHandle(this, (short) i, j);
                    }
                }
                this.nodeLabels = new ArrayList<>(Arrays.asList(manifest.nodeLabels));


                //generate cached set of memory-maps.
                MMapCollection mmaps;
                {
                    ArrayList<Long> offsets = new ArrayList<Long>();
                    offsets.add(manifestPos);
                    for (FlatGraphManifest.StorageProperty p : manifest.properties) {
                        offsets.add(p.qtyOffsets.start);
                        offsets.add(p.valuesOffsets.start);
                    }
                    offsets.sort(Long::compareTo);
                    mmaps = new MMapCollection(offsets, src);
                }

                ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
                ArrayList<Future<?>> futures = new ArrayList<>();
                this.properties = new ArrayList<>(Arrays.asList(new Property[manifest.properties.length]));
                for (int i = 0; i < manifest.properties.length; i += 1) {
                    FlatGraphManifest.StorageProperty p = manifest.properties[i];
                    Property newp = new Property(p.nodeLabel, p.propertyLabel, p.disposition);
                    properties.set(i, newp);
                    newp.cardinality = p.cardinality;
                    newp.type = p.type;
                    if (newp.cardinality != Cardinality.ONE) {
                        ReadJob job = new ReadJob(mmaps.get(p.qtyOffsets), p, newp, true, null, null);
                        newp.qty = job;
                        if (eager) {
                            futures.add(executor.submit(job));
                        }
                    }
                    if (newp.type != ContentType.STRING) {
                        ReadJob job = new ReadJob(mmaps.get(p.valuesOffsets), p, newp, false, null, this.nodes);
                        newp.values = job;
                        if (eager) {
                            futures.add(executor.submit(job));
                        }
                    }
                }


                //obtain stringpool
                int[] stringpoolLens;
                {
                    ZstdDirectBufferDecompressingStream decompressingStream = new ZstdDirectBufferDecompressingStream(mmaps.get(manifest.stringPoolLengths));
                    ByteBuffer outbuf = ByteBuffer.allocateDirect(4 + ZstdDirectBufferDecompressingStream.recommendedTargetBufferSize()).order(ByteOrder.LITTLE_ENDIAN);
                    IntBuffer asInt = outbuf.asIntBuffer();
                    stringpoolLens = new int[(int) (manifest.stringPoolLengths.decompressedLen >> 2)];
                    int outpos = 0;
                    while (outpos < stringpoolLens.length) {
                        decompressingStream.read(outbuf);
                        int readable = outbuf.position() >> 2;
                        asInt.get(stringpoolLens, outpos, readable);
                        outbuf.flip().position(readable * 4);
                        outbuf.compact();
                        outpos += readable;
                        asInt.clear();
                    }
                    decompressingStream.close();
                }
                int maxlen = 0;
                for (int i = 0; i < stringpoolLens.length; i += 1) maxlen = Math.max(maxlen, stringpoolLens[i]);
                String[] strings = new String[stringpoolLens.length + 1];
                {
                    ZstdDirectBufferDecompressingStream decompressingStream = new ZstdDirectBufferDecompressingStream(mmaps.get(manifest.stringPoolData));
                    ByteBuffer directBuf = ByteBuffer.allocateDirect(maxlen + ZstdDirectBufferDecompressingStream.recommendedTargetBufferSize());
                    byte[] heapBuf = new byte[directBuf.capacity()];
                    int i = 1;
                    while (i < strings.length) {
                        decompressingStream.read(directBuf);
                        directBuf.remaining();
                        directBuf.flip();
                        int heapBufPos = 0;
                        int heapBufLim = directBuf.limit();
                        int i0 = i;
                        directBuf.get(heapBuf, 0, heapBufLim);
                        while (i < strings.length && stringpoolLens[i - 1] <= heapBufLim - heapBufPos) {
                            strings[i] = new String(heapBuf, heapBufPos, stringpoolLens[i - 1], StandardCharsets.UTF_8);
                            heapBufPos += stringpoolLens[i - 1];
                            i += 1;
                        }
                        directBuf.position(heapBufPos);
                        directBuf.compact();
                    }
                    decompressingStream.close();

                }

                for (int i = 0; i < manifest.properties.length; i += 1) {
                    FlatGraphManifest.StorageProperty p = manifest.properties[i];
                    Property newp = properties.get(i);
                    if (newp.type == ContentType.STRING) {
                        ReadJob job = new ReadJob(mmaps.get(p.valuesOffsets), p, newp, false, strings, this.nodes);
                        newp.values = job;
                        if (eager) {
                            futures.add(executor.submit(job));
                        }
                    }
                }

                //all jobs submitted, wait for completion
                while (futures.size() > 0) {
                    futures.remove(futures.size() - 1).get();
                }
                executor.shutdown();
                executor.awaitTermination(1L, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }

        //fixme: This should really take a DirectBuffer as input, and we should cache the memorymaps for reuse somewhere.
        public static class ReadJob implements Runnable {
            public String[] strings;
            public ByteBuffer src;
            public FlatGraphManifest.StorageProperty propertyManifest;
            public Property property;
            public boolean isQty;
            public NodeHandle[][] nodeHandles;
            public volatile boolean hasRun = false;

            public ReadJob(ByteBuffer src, FlatGraphManifest.StorageProperty propertyManifest, Property property, boolean isQty, String[] strings, NodeHandle[][] nodeHandles) {
                this.strings = strings;
                this.src = src;
                this.propertyManifest = propertyManifest;
                this.property = property;
                this.isQty = isQty;
                this.nodeHandles = nodeHandles;
            }

            @Override
            public void run() {
                runWithResult();
            }

            public synchronized Object runWithResult() {
                try {
                    ContentType type = isQty ? (propertyManifest.cardinality == Cardinality.MULTI ? ContentType.INT : ContentType.BOOL) : propertyManifest.type;
                    SerializedOffsets offsets = isQty ? propertyManifest.qtyOffsets : propertyManifest.valuesOffsets;

                    if (hasRun) return isQty ? property.qty : property.values;
                    Object res;
                    if (offsets.decompressedLen <= 0) {
                        res = null;
                    } else {
                        ZstdDirectBufferDecompressingStream decompressingStream = new ZstdDirectBufferDecompressingStream(src);
                        //fixme: We need a better way of using the zstd streaming api
                        ByteBuffer outbuf = ByteBuffer.allocateDirect(8 + ZstdDirectBufferDecompressingStream.recommendedTargetBufferSize()).order(ByteOrder.LITTLE_ENDIAN);
                        switch (type) {
                            case BOOL: {
                                int numItems = (int) offsets.decompressedLen;
                                boolean[] out = new boolean[numItems];
                                res = out;
                                byte[] buf2 = new byte[outbuf.capacity()];
                                int outIndex = 0;
                                while (outIndex < numItems) {
                                    int readbytes = decompressingStream.read(outbuf);
                                    outbuf.flip();
                                    outbuf.get(buf2, 0, readbytes);
                                    for (int i = 0; i < readbytes; i += 1) {
                                        out[outIndex + i] = buf2[i] == 0 ? false : true;
                                    }
                                    outIndex += readbytes;
                                    outbuf.compact();
                                }
                                break;
                            }
                            case BYTE: {
                                int numItems = (int) offsets.decompressedLen;
                                byte[] out = new byte[numItems];
                                res = out;
                                int outIndex = 0;
                                while (outIndex < numItems) {
                                    decompressingStream.read(outbuf);
                                    outbuf.flip();
                                    int lim = outbuf.limit();
                                    outbuf.get(out, outIndex, lim);
                                    outbuf.compact();
                                    outIndex += lim;
                                }
                            }
                            break;
                            case SHORT: {
                                int numItems = (int) offsets.decompressedLen >> 1;
                                short[] out = new short[numItems];
                                res = out;
                                ShortBuffer buf2 = outbuf.asShortBuffer();
                                int outIndex = 0;
                                while (outIndex < numItems) {
                                    decompressingStream.read(outbuf);
                                    outbuf.flip();
                                    int lim = outbuf.limit();
                                    buf2.get(out, outIndex, lim >> 1);
                                    buf2.clear();
                                    outbuf.position(lim >> 1 << 1);
                                    outbuf.compact();
                                    outIndex += lim >> 1;
                                }
                            }
                            break;
                            case INT: {
                                int numItems = (int) offsets.decompressedLen >> 2;
                                int[] out = new int[numItems];
                                res = out;
                                IntBuffer buf2 = outbuf.asIntBuffer();
                                int outIndex = 0;
                                while (outIndex < numItems) {
                                    decompressingStream.read(outbuf);
                                    outbuf.flip();
                                    int lim = outbuf.limit();
                                    buf2.get(out, outIndex, lim >> 2);
                                    buf2.clear();
                                    outbuf.position(lim >> 2 << 2);
                                    outbuf.compact();
                                    outIndex += lim >> 2;
                                }
                            }
                            break;
                            case LONG: {
                                int numItems = (int) offsets.decompressedLen >> 3;
                                long[] out = new long[numItems];
                                res = out;
                                LongBuffer buf2 = outbuf.asLongBuffer();
                                int outIndex = 0;
                                while (outIndex < numItems) {
                                    decompressingStream.read(outbuf);
                                    outbuf.flip();
                                    int lim = outbuf.limit();
                                    buf2.get(out, outIndex, lim >> 3);
                                    buf2.clear();
                                    outbuf.position(lim >> 3 << 3);
                                    outbuf.compact();
                                    outIndex += lim >> 3;
                                }
                            }
                            break;
                            case FLOAT: {
                                int numItems = (int) offsets.decompressedLen >> 2;
                                float[] out = new float[numItems];
                                res = out;
                                FloatBuffer buf2 = outbuf.asFloatBuffer();
                                int outIndex = 0;
                                while (outIndex < numItems) {
                                    decompressingStream.read(outbuf);
                                    outbuf.flip();
                                    int lim = outbuf.limit();
                                    buf2.get(out, outIndex, lim >> 2);
                                    buf2.clear();
                                    outbuf.position(lim >> 2 << 2);
                                    outbuf.compact();
                                    outIndex += lim >> 2;
                                }
                            }
                            break;
                            case DOUBLE: {
                                int numItems = (int) offsets.decompressedLen >> 3;
                                double[] out = new double[numItems];
                                res = out;
                                DoubleBuffer buf2 = outbuf.asDoubleBuffer();
                                int outIndex = 0;
                                while (outIndex < numItems) {
                                    decompressingStream.read(outbuf);
                                    outbuf.flip();
                                    int lim = outbuf.limit();
                                    buf2.get(out, outIndex, lim >> 3);
                                    buf2.clear();
                                    outbuf.position(lim >> 3 << 3);
                                    outbuf.compact();
                                    outIndex += lim >> 3;
                                }
                            }
                            break;
                            case STRING: {
                                int numItems = (int) offsets.decompressedLen >> 2;
                                String[] out = new String[numItems];
                                res = out;
                                int outIndex = 0;
                                while (outIndex < numItems) {
                                    decompressingStream.read(outbuf);
                                    outbuf.flip();
                                    int lim = outbuf.limit() >> 2;
                                    for (int i = 0; i < lim; i += 1) {
                                        out[outIndex + i] = strings[outbuf.getInt()];
                                    }
                                    outIndex += lim;
                                    outbuf.compact();
                                }
                            }
                            break;
                            case REF: {
                                int numItems = (int) offsets.decompressedLen >> 3;
                                NodeHandle[] out = new NodeHandle[numItems];
                                res = out;
                                int outIndex = 0;
                                while (outIndex < numItems) {
                                    decompressingStream.read(outbuf);
                                    outbuf.flip();
                                    int lim = outbuf.limit() >> 3;
                                    for (int i = 0; i < lim; i += 1) {
                                        long handleRef = outbuf.getLong();
                                        if (handleRef < 0) {
                                            out[outIndex + i] = null;
                                        } else {
                                            short kind = (short) (handleRef >> 32);
                                            int seq = (int) handleRef;
                                            out[outIndex + i] = nodeHandles[kind][seq];
                                        }
                                    }
                                    outIndex += lim;
                                    outbuf.compact();
                                }
                            }
                            break;
                            default:
                                throw new RuntimeException();
                        }
                        decompressingStream.close();
                    }
                    if (isQty) {
                        if (property.cardinality == Cardinality.MULTI) {
                            int[] intres = (int[]) res;
                            int counter = 0;
                            for (int i = 0; i < intres.length; i += 1) {
                                counter += intres[i];
                                intres[i] = counter;
                                //FIXHERE
                            }
                        }
                        property.qty = res;
                    } else property.values = res;
                    hasRun = true;
                    src = null;
                    strings = null;
                    return res;
                } catch (Exception e) {
                    throw new RuntimeException("except at nodeLabel:" + propertyManifest.nodeLabel + " pLabel:" + propertyManifest.propertyLabel + " disposition:" + propertyManifest.disposition.name(), e);
                }
            }
        }

        public void store(FileChannel dest, boolean destructive) {
            try {
                LinkedHashMap<String, Integer> stringpool = new LinkedHashMap<>();
                FlatGraphManifest manifest = makeManifest();
                this.debugManifest = manifest;

                ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
                AtomicLong filePointer = new AtomicLong(16);
                ArrayList<Future<?>> futures = new ArrayList<>();


                //submit async jobs without any dependencies
                for (int i = 0; i < manifest.properties.length; i += 1) {
                    Property prop = properties.get(i);
                    if (prop.cardinality != Cardinality.ONE) {
                        StoreJob storageJob = new StoreJob(prop.qty, dest, manifest.properties[i].qtyOffsets, filePointer, true);
                        futures.add(executor.submit(storageJob));
                        if (destructive) prop.qty = null;
                    }
                    if (prop.type != ContentType.STRING) {
                        StoreJob storageJob = new StoreJob(prop.values, dest, manifest.properties[i].valuesOffsets, filePointer, false);
                        futures.add(executor.submit(storageJob));
                        if (destructive) prop.values = null;
                    }
                }
                //dedup strings
                for (int j = 0; j < manifest.properties.length; j += 1) {
                    Property prop = properties.get(j);
                    if (prop.type == ContentType.STRING) {
                        if (prop.values instanceof ReadJob) ((ReadJob) prop.values).run();
                        String[] strings = (String[]) prop.values;
                        int[] translated = new int[strings.length];
                        for (int i = 0; i < translated.length; i += 1) {
                            if (strings[i] != null) {
                                translated[i] = stringpool.computeIfAbsent(strings[i], s -> stringpool.size() + 1);
                            }
                        }
                        StoreJob storageJob = new StoreJob(translated, dest, manifest.properties[j].valuesOffsets, filePointer, false);
                        futures.add(executor.submit(storageJob));
                        if (destructive) prop.values = null;
                    }
                }
                //emit pool
                int idx = 0;
                int[] lens = new int[stringpool.size()];
                ByteArrayOutputStream buf = new ByteArrayOutputStream();
                for (String s : stringpool.keySet()) {
                    byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
                    buf.write(bytes);
                    lens[idx] = bytes.length;
                    idx += 1;
                }
                futures.add(executor.submit(new StoreJob(lens, dest, manifest.stringPoolLengths, filePointer, false)));
                futures.add(executor.submit(new StoreJob(buf.toByteArray(), dest, manifest.stringPoolData, filePointer, false)));
                buf = null;
                lens = null;
                //all jobs submitted, wait for completion
                while (futures.size() > 0) {
                    futures.remove(futures.size() - 1).get();
                }
                executor.shutdown();
                executor.awaitTermination(1, TimeUnit.SECONDS);
                //we can now write the file header and the manifest.
                ByteArrayOutputStream iobuf = new ByteArrayOutputStream();
                ObjectOutputStream manifestStream = new ObjectOutputStream(iobuf);
                manifestStream.writeObject(manifest);
                manifestStream.flush();
                ByteBuffer manifestBytes = ByteBuffer.wrap(iobuf.toByteArray());
                long outPos = filePointer.get();
                while (manifestBytes.hasRemaining()) {
                    outPos += dest.write(manifestBytes, outPos);
                }

                ByteBuffer header = ByteBuffer.allocate(16);
                header.order(ByteOrder.LITTLE_ENDIAN).putLong(0xdeadbeefdeadbeefL).putLong(filePointer.get()).position(0);
                outPos = 0;
                while (header.hasRemaining()) {
                    outPos += dest.write(header, outPos);
                }
                dest.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    //for benchmark purposes
    public static void touchGraph(Graph graph) {
        for (Iterator<Node> iter = graph.nodes(); iter.hasNext(); ) {
            Node nx = iter.next();
            if (nx instanceof NodeRef) {
                ((NodeRef) nx).get();
            }
        }
    }

    //for benchmark purposes
    public static void touchGraph(FlatGraphWIP graph) {
        for (Property p : graph.properties) {
            if (p.qty != null && p.qty instanceof FlatGraphWIP.ReadJob) {
                ((FlatGraphWIP.ReadJob) p.qty).run();
            }
            if (p.values instanceof FlatGraphWIP.ReadJob) {
                ((FlatGraphWIP.ReadJob) p.values).run();
            }
        }
    }

    public static FlatGraphWIP makeFlatGraph(Graph graph) {
        FlatGraphWIP res = new FlatGraphWIP();
        Map<Node, NodeHandle> refMap = new HashMap<>();
        Map<String, Integer> counter = new HashMap<>();
        Map<String, Integer> kindMap = new HashMap<>();
        Map<String, ArrayList<NodeHandle>> newRefs = new HashMap<>();
        Map<String, ArrayList<Node>> oldRefs = new HashMap<>();
        ArrayList<Property> properties = new ArrayList<>();
        ArrayList<String> labels = new ArrayList<>();

        for (Iterator<Node> iter = graph.nodes(); iter.hasNext(); ) {
            Node node = iter.next();
            String label = node.label();
            int count = counter.getOrDefault(label, -1) + 1;
            counter.put(label, count);
            int kind = kindMap.getOrDefault(label, -1);
            if (kind == -1) {
                kind = kindMap.size();
                kindMap.put(label, kind);
                labels.add(label);
                newRefs.put(label, new ArrayList<>());
                oldRefs.put(label, new ArrayList<>());
            }
            NodeHandle handle = new NodeHandle(res, (short) kind, count);
            newRefs.get(label).add(handle);
            oldRefs.get(label).add(node);
            refMap.put(node, handle);
        }
        for (String nodeLabel : oldRefs.keySet()) {
            HashMap<String, int[]> propertyCounters = new HashMap<>();
            HashMap<String, ArrayList<Object>> propertyValues = new HashMap<>();

            HashMap<String, int[]> edgeOutCounters = new HashMap<>();
            HashMap<String, ArrayList<NodeHandle>> edgeOutValues = new HashMap<>();

            HashMap<String, int[]> edgeInCounters = new HashMap<>();
            HashMap<String, ArrayList<Object>> edgeInValues = new HashMap<>();
            int nodeCounter = 0;
            ArrayList<Node> oldrefs = oldRefs.get(nodeLabel);
            for (Node node : oldrefs) {
                for (Map.Entry<String, Object> entry : node.propertiesMap().entrySet()) {
                    if (entry.getValue() != null) {
                        if (!propertyCounters.containsKey(entry.getKey())) {
                            propertyCounters.put(entry.getKey(), new int[oldrefs.size()]);
                            propertyValues.put(entry.getKey(), new ArrayList<>());
                        }
                        int[] proplen = propertyCounters.get(entry.getKey());
                        ArrayList<Object> propVals = propertyValues.get(entry.getKey());
                        if (entry.getValue() instanceof ArraySeq) {
                            ArraySeq values = (ArraySeq) entry.getValue();
                            int size = values.size();
                            for (int i = 0; i < size; i = i + 1) {
                                if (values.apply(i) instanceof Node)
                                    propVals.add(refMap.get(values.apply(i)));
                                else
                                    propVals.add(values.apply(i));
                            }
                            proplen[nodeCounter] = size;
                        } else {
                            if (entry.getValue() instanceof Node)
                                propVals.add(refMap.get(entry.getValue()));
                            else
                                propVals.add(entry.getValue());
                            proplen[nodeCounter] = 1;
                        }
                    }
                }
                for (Iterator<Edge> edgeIter = node.outE(); edgeIter.hasNext(); ) {
                    Edge edge = edgeIter.next();
                    String edgeLabel = edge.label();
                    if (!edgeOutCounters.containsKey(edgeLabel)) {
                        edgeOutCounters.put(edgeLabel, new int[oldrefs.size()]);
                        edgeOutValues.put(edgeLabel, new ArrayList<>());
                    }
                    edgeOutValues.get(edgeLabel).add(refMap.get(edge.inNode()));
                    int[] c = edgeOutCounters.get(edgeLabel);
                    c[nodeCounter] += 1;
                }

                for (Iterator<Edge> edgeIter = node.inE(); edgeIter.hasNext(); ) {
                    Edge edge = edgeIter.next();
                    String edgeLabel = edge.label();
                    if (!edgeInCounters.containsKey(edgeLabel)) {
                        edgeInCounters.put(edgeLabel, new int[oldrefs.size()]);
                        edgeInValues.put(edgeLabel, new ArrayList<>());
                    }
                    edgeInValues.get(edgeLabel).add(refMap.get(edge.outNode()));
                    int[] c = edgeInCounters.get(edgeLabel);
                    c[nodeCounter] += 1;
                }

                nodeCounter += 1;
            }
            //we have collected all properties and edges. Now let's compress.
            for (String propertyLabel : propertyCounters.keySet()) {
                Property prop = new Property(nodeLabel, propertyLabel, Disposition.PROPERTY);
                prop.initFromConversion(propertyCounters.get(propertyLabel), propertyValues.get(propertyLabel));
                properties.add(prop);
            }
            for (String propertyLabel : edgeOutCounters.keySet()) {
                Property prop = new Property(nodeLabel, propertyLabel, Disposition.EDGE_OUT);
                prop.initFromConversion(edgeOutCounters.get(propertyLabel), edgeOutValues.get(propertyLabel));
                properties.add(prop);
            }
            for (String propertyLabel : edgeInCounters.keySet()) {
                Property prop = new Property(nodeLabel, propertyLabel, Disposition.EDGE_IN);
                prop.initFromConversion(edgeInCounters.get(propertyLabel), edgeInValues.get(propertyLabel));
                properties.add(prop);
            }
            long[] nodeIds = new long[oldrefs.size()];
            for (int i = 0; i < nodeIds.length; i++) {
                nodeIds[i] = oldrefs.get(i).id();
            }
            Property nodeIdProp = new Property(nodeLabel, "ID", Disposition.PROPERTY);
            nodeIdProp.values = nodeIds;
            nodeIdProp.type = ContentType.LONG;
            nodeIdProp.cardinality = Cardinality.ONE;
            properties.add(nodeIdProp);
        }
        res.properties = properties;
        res.nodeLabels = labels;
        res.nodes = new NodeHandle[labels.size()][];
        NodeHandle[] nodeHandleExample = new NodeHandle[0];
        for (int i = 0; i < labels.size(); i += 1) {
            res.nodes[i] = newRefs.get(labels.get(i)).toArray(nodeHandleExample);
        }
        return res;
    }


    final public static class SerializedOffsets implements Serializable {
        public long start;
        public long compressedLen;
        public long decompressedLen;
    }
}
