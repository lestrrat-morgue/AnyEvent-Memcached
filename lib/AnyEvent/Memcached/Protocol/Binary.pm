package AnyEvent::Memcached::Protocol::Binary;
use Any::Moose;
use namespace::clean -except => qw(meta);

extends 'AnyEvent::Memcached::Protocol';

use constant HEADER_SIZE => 24;
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

# Constants
use constant +{
#    Magic numbers
    REQ_MAGIC       => 0x80,
    RES_MAGIC       => 0x81,

#    Status Codes
#    0x0000  No error
#    0x0001  Key not found
#    0x0002  Key exists
#    0x0003  Value too large
#    0x0004  Invalid arguments
#    0x0005  Item not stored
#    0x0006  Incr/Decr on non-numeric value.
    ST_SUCCESS      => 0x0000,
    ST_NOT_FOUND    => 0x0001,
    ST_EXISTS       => 0x0002,
    ST_TOO_LARGE    => 0x0003,
    ST_INVALID      => 0x0004,
    ST_NOT_STORED   => 0x0005,
    ST_NON_NUMERIC  => 0x0006,

#    Opcodes
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
    $OPAQUE = 0xffffffff;
}

sub _encode_message {
    my ($opcode, $key, $extras, $data_type, $reserved, $cas, $body) = @_;

    use bytes;

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
    my $i4 = $OPAQUE;
    if ($OPAQUE == 0xffffffff) {
        $OPAQUE = 0;
    } else {
        $OPAQUE++;
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
    if (bytes::length($message) > HEADER_SIZE) {
        confess "header size assertion failed";
    }

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
    my $key_length = $i1 & 0x0000ffff;
    my $extra_length = ($i2 & 0xff000000) >> 24;
    my $data_type = ($i2 & 0x00ff0000) >> 8;
    my $status = $i2 & 0x0000ffff;
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

    return ($magic, $opcode, $key_length, $extra_length, $status, $data_type, $total_body_length, $opaque, $cas);
}

sub _status_str {
    my $status = shift;
    my %strings = (
        ST_SUCCESS() => "Success",
        ST_NOT_FOUND() => "Not found",
        ST_EXISTS() => "Exists",
        ST_TOO_LARGE() => "Too Large",
        ST_INVALID() => "Invalid Arguments",
        ST_NOT_STORED() => "Not Stored",
        ST_NON_NUMERIC() => "Incr/Decr on non-numeric variables"
    );
    return $strings{$status};
}

{
    my $generator = sub {
        my ($self, $cmd) = @_;
        sub {
            my ($key, $value, $exptime, $noreply, $cb) = @_;

            my $handle = $self->get_handle_for( $key );
            my $memcached = $self->memcached;

            my ($write_data, $write_len, $flags, $expires) =
                $self->prepare_value( $cmd, $value, $exptime );

            my $extras = pack('N2', $flags, $expires);

            $handle->push_write( 
                _encode_message(MEMD_ADD, $key, $extras, undef, undef, 
                    # allow this to be set from outside
                    undef, # $cas
                    $write_data
                )
            );
            $handle->push_read(chunk => HEADER_SIZE, sub {
                my ($handle, $header) = @_;
                my ($magic, $opcode, $key_length, $extra_length, $status, $data_type, $total_body_length, $opaque, $cas) = _decode_header($header);

                if ($magic != RES_MAGIC) {
                    $cb->(undef, "Response magic is not of expected value");
                    $self->memcached->drain_queue;
                    return;
                } 

                $cb->($status);
                $self->memcached->drain_queue;
            });
        }
    };

    sub _build_add_cb {
        my $self = shift;
        return $generator->($self, "add");
    }
}

sub _build_delete_cb {
    my $self = shift;

    return sub {
        my ($key, $noreply, $cb) = @_;

        my $handle = $self->get_handle_for($key);

        $handle->push_write( _encode_message(MEMD_DELETE, $key) );
        $handle->push_read(chunk => HEADER_SIZE, sub {
            my ($handle, $header) = @_;
            my ($magic, $opcode, $key_length, $extra_length, $status, $data_type, $total_body_length, $opaque, $cas) = _decode_header($header);

            if ($magic != RES_MAGIC) {
                $cb->(undef, "Response magic is not of expected value");
                $self->memcached->drain_queue;
                return;
            } 

            $cb->($status);
            $self->memcached->drain_queue;
        });
    }
}

sub _build_get_multi_cb {
    my $self = shift;

    return sub {
        my ($keys, $cb, $cb_caller) = @_;

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

        my %result;
        my $cv = AE::cv { 
            # This trigger should be called when ALL keys have responded
            $cb_caller->($cb, \%result);
            $self->memcached->drain_queue;
        };
        foreach my $list (values %handle2keys) {
            my ($handle, @keys) = @$list;
            foreach my $data ( map { _encode_message(MEMD_GETK, $_) } @keys ) {
                $cv->begin;
                $handle->push_write($data);
                $handle->push_read(chunk => HEADER_SIZE, sub {
                    my ($handle, $header) = @_;

                    my ($magic, $opcode, $key_length, $extra_length, $status, $data_type, $total_body_length, $opaque, $cas) = _decode_header($header);

                    if ($magic != RES_MAGIC) {
                        $cb->(undef, "Response magic is not of expected value");
                        $cv->end;
                        return;
                    } 

                    if ($status != 0) {
                        $cb->(undef, _status_str($status));
                        $cv->end;
                        return;
                    }

                    if ($total_body_length) {
                        $cv->begin;
                        $handle->push_read(chunk => $total_body_length, sub {
                            my ($handle, $body) = @_;

                            $body = unpack('a*', $body);
                            my $extra = $extra_length ? substr($body, 0, $extra_length, '') : undef;
                            my $key = $key_length ? substr($body, 0, $key_length, '') : undef;
                            $result{ $key } = $body || undef;

                            $cv->end;
                        });
                    }

                    $cv->end;
                });

                $handle->push_write( _noop() );
            }
        }
    };
}

sub prepare_handle {
    my ($self, $fh) = @_;
    binmode($fh);
}

__PACKAGE__->meta->make_immutable();

1;
