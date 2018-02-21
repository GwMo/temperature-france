# qnap to seagate
rsync -ni -ru --delete-after --progress /cygdrive/p/0.raw/  /cygdrive/f/P055.Ian/0.raw/
rsync -ni -ru --delete-after --progress /cygdrive/p/1.work/ /cygdrive/f/P055.Ian/1.work/

# qnap to biosphere
rsync -ni -rt --delete-after --progress /cygdrive/p/0.raw/aster/               ~/My\ Documents/P055.Ian/0.raw/aster/
rsync -ni -rt --delete-after --progress /cygdrive/p/0.raw/copernicus/          ~/My\ Documents/P055.Ian/0.raw/copernicus/
rsync -ni -rt --delete-after --progress /cygdrive/p/0.raw/ign/                 ~/My\ Documents/P055.Ian/0.raw/ign/
rsync -ni -rt --delete-after --progress /cygdrive/p/0.raw/insee/               ~/My\ Documents/P055.Ian/0.raw/insee/
rsync -ni -rt --delete-after --progress /cygdrive/p/0.raw/joly_climats_france/ ~/My\ Documents/P055.Ian/0.raw/joly_climats_france/
rsync -ni -rt --delete-after --progress /cygdrive/p/0.raw/meteo_france/        ~/My\ Documents/P055.Ian/0.raw/meteo_france/
rsync -ni -rt --delete-after --progress /cygdrive/p/1.work/                    ~/My\ Documents/P055.Ian/1.work/


# biosphere to seagate
rsync -ni -ru --delete-after --progress ~/My\ Documents/P055.Ian/0.raw/aster/               /cygdrive/f/P055.Ian/0.raw/aster/
rsync -ni -ru --delete-after --progress ~/My\ Documents/P055.Ian/0.raw/copernicus/          /cygdrive/f/P055.Ian/0.raw/copernicus/
rsync -ni -ru --delete-after --progress ~/My\ Documents/P055.Ian/0.raw/ign/                 /cygdrive/f/P055.Ian/0.raw/ign/
rsync -ni -ru --delete-after --progress ~/My\ Documents/P055.Ian/0.raw/insee/               /cygdrive/f/P055.Ian/0.raw/insee/
rsync -ni -ru --delete-after --progress ~/My\ Documents/P055.Ian/0.raw/joly_climats_france/ /cygdrive/f/P055.Ian/0.raw/joly_climats_france/
rsync -ni -ru --delete-after --progress ~/My\ Documents/P055.Ian/0.raw/meteo_france/        /cygdrive/f/P055.Ian/0.raw/meteo_france/
rsync -ni -ru --delete-after --progress ~/My\ Documents/P055.Ian/1.work/                    /cygdrive/f/P055.Ian/1.work/

# biosphere to qnap
rsync -ni -rt --delete-after --progress ~/My\ Documents/P055.Ian/0.raw/aster/               /cygdrive/p/0.raw/aster/
rsync -ni -rt --delete-after --progress ~/My\ Documents/P055.Ian/0.raw/copernicus/          /cygdrive/p/0.raw/copernicus/
rsync -ni -rt --delete-after --progress ~/My\ Documents/P055.Ian/0.raw/ign/                 /cygdrive/p/0.raw/ign/
rsync -ni -rt --delete-after --progress ~/My\ Documents/P055.Ian/0.raw/insee/               /cygdrive/p/0.raw/insee/
rsync -ni -rt --delete-after --progress ~/My\ Documents/P055.Ian/0.raw/joly_climats_france/ /cygdrive/p/0.raw/joly_climats_france/
rsync -ni -rt --delete-after --progress ~/My\ Documents/P055.Ian/0.raw/meteo_france/        /cygdrive/p/0.raw/meteo_france/
rsync -ni -rt --delete-after --progress ~/My\ Documents/P055.Ian/1.work/                    /cygdrive/p/1.work/


################################################################################
#
# # qnap (network NTFS) -> biosphere (local NTFS)
# Don't use -a -p -g -o
#   Can't set group and owner on NTFS
#   Permissions from qnap aren't useful (b/c they can't be set)
# Use -t
#   Set modification time
#
# rsync -ni -rt --delete-after --progress
#
# # biosphere (local NTFS) -> qnap (network NTFS)
# Don't use -a -p -g -o
#   Can't set group and owner on NTFS
#   Can't set permissions on network NTFS
# Use -t
#   Set modification time
#
# rsync -ni -rt --delete-after --progress
#
#
# # biosphere (local NTFS) or qnap (network NTFS) -> seagate (local exFAT)
# Don't use -a -p -t -g -o
#   Risky to transfer group + owner b/c external drive may be connected to other systems
#   Can't set permissions or modification time on exFAT
# Use -u
#   Skip dest files that are newer than source
#   Needed b/c on seagate file modification time is set to transfer time, so
#   files will always be newer (i.e. re-running rsync will always update times)
#
# rsync -ni -ru --delete-after --progress
#
# # seagate (local exFAT) -> biosphere (local NTFS) or qnap (network NTFS)
# Don't use -a -p -g -o
#   Can't set group and owner on NTFS
#   Permissions from exFAT aren't useful (b/c they can't be set)
# Use -t
#   Set modification time
#
# rsync -ni -rt --delete-after --progress
#
################################################################################
#
# -c, --checksum        skip based on checksum, not mod time & size
# -i, --itemize-changes output a change-summary for all updates
# -n, --dry-run         perform a trial run with no changes made
# -v, --verbose         verbose output
# -z, --compress        compress file data during the transfer
#
#     --delete-after    receiver deletes after transfer, not during
#     --progress        show progress during transfer
#
# -a, --archive         archive mode; equals -rlptgoD (no -H,-A,-X)
# -r, --recursive       recurse into directories
# -l, --links           copy symlinks as symlinks
# -p, --perms           preserve permissions
# -t, --times           preserve modification times
# -g, --group           preserve group
# -o, --owner           preserve owner (super-user only)
# -D                    same as --devices --specials
#     --devices         preserve device files (super-user only)
#     --specials        preserve special files
#
################################################################################
#
# Itemized output
# https://stackoverflow.com/questions/4493525/rsync-what-means-the-f-on-rsync-logs
#
# YXcstpoguax
# <.......... = received at source
# >.......... = sent to destination
# c.......... = changed/created locally
# h.......... = is hard link
# ........... = no update, but attributes may be modified
# *.......... = message follows (e.g. *deleting)
#
# .f......... = file
# .d......... = directory
# .L......... = symlink
# .D......... = device
# .S......... = special (e.g. named sockets and fifos)
#
# ..+++++++++ = item is created
# ..          = item is identical
# ..????????? = unknown attribute
# ........... = no attribute change
#
# ..c........ = different checksum or symlink/device/special has changed value
# ...s....... = size differs; file will be sent
# ....t...... = time differs; file will be sent and time set
# ....T...... = time differs; file will be sent and time set to transfer time
# .....p..... = permissions will be updated
# ......o.... = owner will be updated
# .......g... = group will be updated
# ........u.. = [reserved for future use]
# .........a. = ACL will be updated
# ..........x = extended attributes will be updated
#
################################################################################
#
# Options
#  -v, --verbose               increase verbosity
#      --info=FLAGS            fine-grained informational verbosity
#      --debug=FLAGS           fine-grained debug verbosity
#      --msgs2stderr           special output handling for debugging
#  -q, --quiet                 suppress non-error messages
#      --no-motd               suppress daemon-mode MOTD (see manpage caveat)
#  -c, --checksum              skip based on checksum, not mod-time & size
#  -a, --archive               archive mode; equals -rlptgoD (no -H,-A,-X)
#      --no-OPTION             turn off an implied OPTION (e.g. --no-D)
#  -r, --recursive             recurse into directories
#  -R, --relative              use relative path names
#      --no-implied-dirs       don't send implied dirs with --relative
#  -b, --backup                make backups (see --suffix & --backup-dir)
#      --backup-dir=DIR        make backups into hierarchy based in DIR
#      --suffix=SUFFIX         set backup suffix (default ~ w/o --backup-dir)
#  -u, --update                skip files that are newer on the receiver
#      --inplace               update destination files in-place (SEE MAN PAGE)
#      --append                append data onto shorter files
#      --append-verify         like --append, but with old data in file checksum
#  -d, --dirs                  transfer directories without recursing
#  -l, --links                 copy symlinks as symlinks
#  -L, --copy-links            transform symlink into referent file/dir
#      --copy-unsafe-links     only "unsafe" symlinks are transformed
#      --safe-links            ignore symlinks that point outside the source tree
#      --munge-links           munge symlinks to make them safer (but unusable)
#  -k, --copy-dirlinks         transform symlink to a dir into referent dir
#  -K, --keep-dirlinks         treat symlinked dir on receiver as dir
#  -H, --hard-links            preserve hard links
#  -p, --perms                 preserve permissions
#  -E, --executability         preserve the file's executability
#      --chmod=CHMOD           affect file and/or directory permissions
#  -A, --acls                  preserve ACLs (implies --perms)
#  -o, --owner                 preserve owner (super-user only)
#  -g, --group                 preserve group
#      --devices               preserve device files (super-user only)
#      --specials              preserve special files
#  -D                          same as --devices --specials
#  -t, --times                 preserve modification times
#  -O, --omit-dir-times        omit directories from --times
#  -J, --omit-link-times       omit symlinks from --times
#      --super                 receiver attempts super-user activities
#  -S, --sparse                handle sparse files efficiently
#      --preallocate           allocate dest files before writing them
#  -n, --dry-run               perform a trial run with no changes made
#  -W, --whole-file            copy files whole (without delta-xfer algorithm)
#  -x, --one-file-system       don't cross filesystem boundaries
#  -B, --block-size=SIZE       force a fixed checksum block-size
#  -e, --rsh=COMMAND           specify the remote shell to use
#      --rsync-path=PROGRAM    specify the rsync to run on the remote machine
#      --existing              skip creating new files on receiver
#      --ignore-existing       skip updating files that already exist on receiver
#      --remove-source-files   sender removes synchronized files (non-dirs)
#      --del                   an alias for --delete-during
#      --delete                delete extraneous files from destination dirs
#      --delete-before         receiver deletes before transfer, not during
#      --delete-during         receiver deletes during the transfer
#      --delete-delay          find deletions during, delete after
#      --delete-after          receiver deletes after transfer, not during
#      --delete-excluded       also delete excluded files from destination dirs
#      --ignore-missing-args   ignore missing source args without error
#      --delete-missing-args   delete missing source args from destination
#      --ignore-errors         delete even if there are I/O errors
#      --force                 force deletion of directories even if not empty
#      --max-delete=NUM        don't delete more than NUM files
#      --max-size=SIZE         don't transfer any file larger than SIZE
#      --min-size=SIZE         don't transfer any file smaller than SIZE
#      --partial               keep partially transferred files
#      --partial-dir=DIR       put a partially transferred file into DIR
#      --delay-updates         put all updated files into place at transfer's end
#  -m, --prune-empty-dirs      prune empty directory chains from the file-list
#      --numeric-ids           don't map uid/gid values by user/group name
#      --usermap=STRING        custom username mapping
#      --groupmap=STRING       custom groupname mapping
#      --chown=USER:GROUP      simple username/groupname mapping
#      --timeout=SECONDS       set I/O timeout in seconds
#      --contimeout=SECONDS    set daemon connection timeout in seconds
#  -I, --ignore-times          don't skip files that match in size and mod-time
#  -M, --remote-option=OPTION  send OPTION to the remote side only
#      --size-only             skip files that match in size
#      --modify-window=NUM     compare mod-times with reduced accuracy
#  -T, --temp-dir=DIR          create temporary files in directory DIR
#  -y, --fuzzy                 find similar file for basis if no dest file
#      --compare-dest=DIR      also compare destination files relative to DIR
#      --copy-dest=DIR         ... and include copies of unchanged files
#      --link-dest=DIR         hardlink to files in DIR when unchanged
#  -z, --compress              compress file data during the transfer
#      --compress-level=NUM    explicitly set compression level
#      --skip-compress=LIST    skip compressing files with a suffix in LIST
#  -C, --cvs-exclude           auto-ignore files the same way CVS does
#  -f, --filter=RULE           add a file-filtering RULE
#  -F                          same as --filter='dir-merge /.rsync-filter'
#                              repeated: --filter='- .rsync-filter'
#      --exclude=PATTERN       exclude files matching PATTERN
#      --exclude-from=FILE     read exclude patterns from FILE
#      --include=PATTERN       don't exclude files matching PATTERN
#      --include-from=FILE     read include patterns from FILE
#      --files-from=FILE       read list of source-file names from FILE
#  -0, --from0                 all *-from/filter files are delimited by 0s
#  -s, --protect-args          no space-splitting; only wildcard special-chars
#      --address=ADDRESS       bind address for outgoing socket to daemon
#      --port=PORT             specify double-colon alternate port number
#      --sockopts=OPTIONS      specify custom TCP options
#      --blocking-io           use blocking I/O for the remote shell
#      --stats                 give some file-transfer stats
#  -8, --8-bit-output          leave high-bit chars unescaped in output
#  -h, --human-readable        output numbers in a human-readable format
#      --progress              show progress during transfer
#  -P                          same as --partial --progress
#  -i, --itemize-changes       output a change-summary for all updates
#      --out-format=FORMAT     output updates using the specified FORMAT
#      --log-file=FILE         log what we're doing to the specified FILE
#      --log-file-format=FMT   log updates using the specified FMT
#      --password-file=FILE    read daemon-access password from FILE
#      --list-only             list the files instead of copying them
#      --bwlimit=RATE          limit socket I/O bandwidth
#      --outbuf=N|L|B          set output buffering to None, Line, or Block
#      --write-batch=FILE      write a batched update to FILE
#      --only-write-batch=FILE like --write-batch but w/o updating destination
#      --read-batch=FILE       read a batched update from FILE
#      --protocol=NUM          force an older protocol version to be used
#      --iconv=CONVERT_SPEC    request charset conversion of filenames
#      --checksum-seed=NUM     set block/file checksum seed (advanced)
#      --noatime               do not alter atime when opening source files
#  -4, --ipv4                  prefer IPv4
#  -6, --ipv6                  prefer IPv6
#      --version               print version number
# (-h) --help                  show this help (-h is --help only if used alone)
