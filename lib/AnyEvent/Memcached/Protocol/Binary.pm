package AnyEvent::Memcached::Protocol::Binary;
use Any::Moose;
use namespace::clean -except => qw(meta);

extends 'AnyEvent::Memcached::Protocol';

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
#     12| Message ID                                                    |
#       +---------------+---------------+---------------+---------------+
#     Total 16 bytes
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
#     12| Message ID                                                    |
#       +---------------+---------------+---------------+---------------+
#     Total 16 bytes
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
#   Message ID          Will be copied back to you in the response.
#                       FIXME: Can this be used to organize [UDP]
#                       packets?

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

sub _build_get_multi_cb {
    my $self = shift;

    return sub {
        my ($key, $cb) = @_;

        # XXX as of this commit, my puny attemp doesn't work.
        # I'm too embarassed to show it here, so screw you.
        my $data = $self->make_message( MEMD_GET, $key );

        my $handle = $self->get_handle_for( $key );

        $handle->push_write($data);
        $handle->push_read(chunk => 16, sub {
            my ($handle, $header) = @_;

            my ($first_long, $second_long) = unpack('N2', $header);
            my $magic = $first_long >> 24;
            my $opcode = ($first_long & 0x00ff0000) >> 16;
            my $status = $first_long & 0x0000ffff;

            if ($magic != RES_MAGIC) {
                $cb->(undef, "Response magic is not of expected value");
                return;
            } 
            $cb->( $magic );
        });
    };
}

sub prepare_handle {
    my ($self, $fh) = @_;
    binmode($fh);
}

sub make_message {
}

__PACKAGE__->meta->make_immutable();

1;
