package AnyEvent::Memcached::Protocol::Binary;
use Any::Moose;
use namespace::clean -except => qw(meta);

extends 'AnyEvent::Memcached::Protocol';

use constant HAS_64BIT => do {
    no strict;
    require Config;
    $Config{use64bitint};
};

#   General format of a packet:
#
#     Byte/     0       |       1       |       2       |       3       |
#        /              |               |               |               |
#       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
#       +---------------+---------------+---------------+---------------+
#      0/ HEADER                                                        /
#       /                                                               /
#       /                                                               /
#       /                                                               /
#       +---------------+---------------+---------------+---------------+
#     16/ COMMAND-SPECIFIC EXTRAS (as needed)                           /
#      +/  (note length in th extras length header field)               /
#       +---------------+---------------+---------------+---------------+
#      m/ Key (as needed)                                               /
#      +/  (note length in key length header field)                     /
#       +---------------+---------------+---------------+---------------+
#      n/ Value (as needed)                                             /
#      +/  (note length is total body length header field, minus        /
#      +/   sum of the extras and key length body fields)               /
#       +---------------+---------------+---------------+---------------+
#      Total 16 bytes
#
#   Request header:
#
#     Byte/     0       |       1       |       2       |       3       |
#        /              |               |               |               |
#       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
#       +---------------+---------------+---------------+---------------+
#      0| Magic         | Opcode        | Key length                    |
#       +---------------+---------------+---------------+---------------+
#      4| Extras length | Data type     | Reserved                      |
#       +---------------+---------------+---------------+---------------+
#      8| Total body length                                             |
#       +---------------+---------------+---------------+---------------+
#     12| Opaque                                                        |
#       +---------------+---------------+---------------+---------------+
#     16| CAS                                                           |
#       |                                                               |
#       +---------------+---------------+---------------+---------------+
#       Total 24 bytes
#
#   Response header:
#
#     Byte/     0       |       1       |       2       |       3       |
#        /              |               |               |               |
#       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
#       +---------------+---------------+---------------+---------------+
#      0| Magic         | Opcode        | Status                        |
#       +---------------+---------------+---------------+---------------+
#      4| Extras length | Data type     | Reserved                      |
#       +---------------+---------------+---------------+---------------+
#      8| Total body length                                             |
#       +---------------+---------------+---------------+---------------+
#     12| Opaque                                                        |
#       +---------------+---------------+---------------+---------------+
#     16| CAS                                                           |
#       |                                                               |
#       +---------------+---------------+---------------+---------------+
#       Total 24 bytes
#
#   Header fields:
#   Magic               Magic number.
#   Opcode              Command code.
#   Key length          Length in bytes of the text key that follows the
#                       command extras.
#   Status              Status of the response (non-zero on error).
#   Extras length       Length in bytes of the command extras.
#   Data type           Reserved for future use (Sean is using this
#                       soon).
#   Reserved            Really reserved for future use (up for grabs).
#   Total body length   Length in bytes of extra + key + value.
#   Opaque              Will be copied back to you in the response.
#   CAS                 Data version check.
use constant +{
    REQ_MAGIC       => 0x80,
    RES_MAGIC       => 0x81,
    MEMD_GET        => 0x00,
    MEMD_SET        => 0x01,
    MEMD_ADD        => 0x02,
    MEMD_REPLACE    => 0x03,
    MEMD_DELETE     => 0x04,
    MEMD_INCREMENT  => 0x05,
    MEMD_DECREMENT  => 0x06,
    MEMD_QUIT       => 0x07,
    MEMD_FLUSH      => 0x08,
    MEMD_GETQ       => 0x09,
    MEMD_NOOP       => 0x0A,
    MEMD_VERSION    => 0x0B,
    MEMD_GETK       => 0x0C,
    MEMD_GETKQ      => 0x0D,
    MEMD_APPEND     => 0x0E,
    MEMD_PREPEND    => 0x0F,
    MEMD_STAT       => 0x10,
    MEMD_SETQ       => 0x11,
    MEMD_ADDQ       => 0x12,
    MEMD_REPLACEQ   => 0x13,
    MEMD_DELETEQ    => 0x14,
    MEMD_INCREMENTQ => 0x15,
    MEMD_DECREMENTQ => 0x16,
    MEMD_QUITQ      => 0x17,
    MEMD_FLUSHQ     => 0x18,
    MEMD_APPENDQ    => 0x19,
    MEMD_PREPENDQ   => 0x1A,
    RAW_BYTES       => 0x00,
};

my $OPAQUE;
BEGIN {
    $OPAQUE = 0;
}

sub _encode_message {
    my ($opcode, $key, $extras, $data_type, $reserved, $cas, $body) = @_;

    my $key_length = defined $key ? bytes::length($key) : 0;
    # first 4 bytes (long)
    my $i1 = 0;
    $i1 ^= REQ_MAGIC << 24;
    $i1 ^= $opcode << 16;
    $i1 ^= $key_length;

    # second 4 bytes
    my $extra_length = defined $extras ? bytes::length($extras) : 0;
    my $i2 = 0;
    $i2 ^= $extra_length << 24;
    # $data_type and $reserved are not used currently

    # third 4 bytes
    my $body_length  = defined $body ? bytes::length($body) : 0;
    my $i3 = $body_length + $key_length + $extra_length;

    # this is the opaque value, which will be returned with the response
    my $i4 = $OPAQUE + 1;
    if ($OPAQUE == 0xffffffff) {
        $OPAQUE = 0;
    }

    # CAS is 64 bit, which is troublesome on 32 bit architectures.
    # we will NOT allow 64 bit CAS on 32 bit machines for now.
    # better handling by binary-adept people are welcome
    $cas ||= 0;
    my ($i5, $i6);
    if (HAS_64BIT) {
        no warnings;
        $i5 = 0xffffffff00000000 & $cas;
        $i6 = 0x00000000ffffffff & $cas;
    } else {
        $i5 = 0x00000000;
        $i6 = $cas;
    }

    my $message = pack( 'N6', $i1, $i2, $i3, $i4, $i5, $i6 );

    if ($extra_length) {
        $message .= $extras;
    }
    if ($key_length) {
        $message .= pack('a*', $key);
    }
    if ($body_length) {
        $message .= pack('a*', $body);
    }

    return $message;
}

use constant _noop => _encode_message(MEMD_NOOP, undef, undef, undef, undef, undef, undef);

sub _decode_header {
    my $header = shift;

    my ($i1, $i2, $i3, $i4, $i5, $i6) = unpack('N6', $header);
    my $magic = $i1 >> 24;
    my $opcode = ($i1 & 0x00ff0000) >> 16;
    my $status = $i1 & 0x0000ffff;
    my $extra_length = ($i2 & 0xff000000) >> 16;
    my $data_type = undef; # not used
    my $reserved  = undef; # not used
    my $total_body_length = $i3;
    my $opaque = $i4;

    my $cas;
    if (HAS_64BIT) {
        $cas = $i5 << 32;
        $cas += $i6;
    } else {
        warn "overflow on CAS" if ($i5 || 0) != 0;
        $cas = $i6;
    }

    return ($magic, $opcode, $status, $extra_length, $data_type, $reserved, $total_body_length, $opaque, $cas);
}

sub _build_get_multi_cb {
    my $self = shift;

    return sub {
        my ($keys, $cb) = @_;

        # organize the keys by handle
        my %handle2keys;
            
        foreach my $key (@$keys) {
            my $handle = $self->get_handle_for( $key );
            my $list = $handle2keys{ $handle };
            if (! $list) {
                $handle2keys{$handle} = [ $handle, $key ];
            } else {
                push @$list, $key;
            }
        }

        foreach my $list (values %handle2keys) {
            my ($handle, @keys) = @$list;
            foreach my $data ( map { _encode_message(MEMD_GETK, $_) } @keys ) {
                $handle->push_write($data);
                $handle->push_read(chunk => 16, sub {
                    my ($handle, $header) = @_;

                    my ($magic, $opcode, $status, $extra_length, $data_type, $reserved, $total_body_length, $opaque, $cas) = _decode_header($header);

                    if ($magic != RES_MAGIC) {
                        $cb->(undef, "Response magic is not of expected value");
                        return;
                    } 

                    if ($status != 0) {
warn "Error status: $status";
                        $cb->(undef, "Error status");
                        return;
                    }

                    if ($extra_length) {
                        $handle->push_read(chunk => $extra_length, sub {
                            warn "extra = $_[1]";
                        });
                    }

                    if ($total_body_length) {
                        $handle->push_read(chunk => $total_body_length, sub {
                            warn "body = $_[1]";
                        });
                    }

#                    $cb->( $magic );
                });
            }
            $handle->push_write( _noop() );
        }
    };
}

sub prepare_handle {
    my ($self, $fh) = @_;
    binmode($fh);
}

__PACKAGE__->meta->make_immutable();

1;
