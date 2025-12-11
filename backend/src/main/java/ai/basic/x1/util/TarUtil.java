package ai.basic.x1.util;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.utils.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class TarUtil {

    /**
     * 디렉토리를 TAR로 아카이빙 (압축 없음)
     */
    public static File tar(File sourceDir, File tarFile) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(tarFile);
             TarArchiveOutputStream tarOut = new TarArchiveOutputStream(fos)) {
    
            tarOut.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
            addFilesToTar(tarOut, sourceDir, "");
        }
        return tarFile;
    }

    /**
     * 디렉토리를 TAR.GZ 로 아카이빙 + 압축
     */
    public static File tarGz(File sourceDir, File tarGzFile) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(tarGzFile);
             GzipCompressorOutputStream gzipOut = new GzipCompressorOutputStream(fos);
             TarArchiveOutputStream tarOut = new TarArchiveOutputStream(gzipOut)) {
    
            tarOut.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
            addFilesToTar(tarOut, sourceDir, "");
        }
        return tarGzFile;
    }

    private static void addFilesToTar(TarArchiveOutputStream tarOut, File file, String base) throws IOException {
        String entryName = base + file.getName();
        TarArchiveEntry entry = new TarArchiveEntry(file, entryName);

        tarOut.putArchiveEntry(entry);

        if (file.isFile()) {
            try (var fis = new java.io.FileInputStream(file)) {
                IOUtils.copy(fis, tarOut);
            }
            tarOut.closeArchiveEntry();
        } else {
            tarOut.closeArchiveEntry();
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    addFilesToTar(tarOut, child, entryName + "/");
                }
            }
        }
    }
}