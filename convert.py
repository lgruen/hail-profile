import json
import hail as hl

hl.init(default_reference='GRCh38')

ht = hl.read_table('/home/leo/gnomad_popmax_af.ht')

unblocked_uncompressed = json.dumps({'name': 'StreamBufferSpec'})

blocked_uncompressed = json.dumps({
    'name': 'BlockingBufferSpec',
    'blockSize': 64 * 1024,
    'child': {'name': 'StreamBufferSpec'}
})

fast_codec_spec = json.dumps({
    'name': 'BlockingBufferSpec',
    'blockSize': 64 * 1024,
    'child': {
        'name': 'LZ4FastBlockBufferSpec',
        'blockSize': 64 * 1024,
        'child': {'name': 'StreamBlockBufferSpec'}
    }
})

ht.write('/home/leo/gnomad_popmax_af_unblocked_uncompressed.ht', _codec_spec=unblocked_uncompressed)
ht.write('/home/leo/gnomad_popmax_af_blocked_uncompressed.ht', _codec_spec=blocked_uncompressed)
ht.write('/home/leo/gnomad_popmax_af_fast.ht', _codec_spec=fast_codec_spec)
