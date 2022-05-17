# You must start Generator.Gui from VLTK before executing this script (https://github.com/sandersaares/video-latency-toolkit).

# Media server supports other paths, as well (just change the "hls" component to publish multiple media streams).
$url = "http://localhost:5005/files/hls/media.m3u8"

# NB! This needs a low latency connection to the media server.
# 1 second segments are the smallest FFmpeg seems to reliably produce for us here. 0.5 fails playback.
$expression = "ffmpeg --% -fflags nobuffer -re -f lavfi -i sine=220:4       -fflags nobuffer -re -f gdigrab -framerate 30 -i `"title=Video latency toolkit - signal generator`"               -map 0 -ac 1 -ar 32000 -c:a aac -b:a 64k       -map 1 -c:v libx264  -pix_fmt yuv420p -profile:v high -preset fast -sc_threshold 0 -x264-params keyint=60:min-keyint=60:rc-lookahead=1:bframes=0:sliced-threads=1 -b:v 1500k -minrate:v 1500k -maxrate:v 1500k -bufsize:v 150k -s 512x512             -f hls -hls_time 2 -hls_list_size 10 -method POST -hls_segment_type fmp4 -http_persistent 1 -ignore_io_errors 1  $url"

Invoke-Expression $expression