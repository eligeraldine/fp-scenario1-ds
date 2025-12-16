#!/bin/bash

echo "--- Membersihkan Server Redis ---"

# Menghentikan semua proses redis-server
pkill redis-server
echo "✅ Semua proses redis-server dihentikan."

# Menghapus file database dan log
# Menggunakan sudo jika file mungkin dibuat oleh root (terutama di lingkungan GNS/Linux)
rm -f *.log dump*.rdb temp-*.rdb
echo "✅ File *.log dan RDB dihapus."

echo "--- Pembersihan Selesai ---"
